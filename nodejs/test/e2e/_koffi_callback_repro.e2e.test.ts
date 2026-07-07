/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

/**
 * Standalone repro for the in-process FFI hang: does koffi deliver a foreign-thread
 * callback to JS when the main event loop is otherwise idle (only a keep-alive timer)?
 *
 * No SDK, no runtime cdylib, no JSON-RPC — just a tiny C shared library whose exported
 * function spawns a background thread that, after a delay, invokes a koffi-registered
 * callback. Mirrors how the runtime's worker-reader thread invokes our outbound
 * callback while the SDK sits idle awaiting a response.
 *
 * Compiled on the fly with cc/clang (Linux + macOS). Skipped where no C compiler /
 * POSIX threads are available (e.g. Windows without a toolchain).
 */

import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import koffi from "koffi";

const C_SOURCE = `
#include <pthread.h>
#include <unistd.h>

typedef void (*ping_cb)(int seq);

static ping_cb g_cb;
static int g_delay_ms;
static int g_count;

static void *pinger(void *arg) {
    (void)arg;
    for (int i = 0; i < g_count; i++) {
        usleep(g_delay_ms * 1000);
        if (g_cb) g_cb(i);
    }
    return 0;
}

// Spawn a detached background thread that invokes cb 'count' times, 'delay_ms' apart.
void start_pinger(ping_cb cb, int delay_ms, int count) {
    g_cb = cb;
    g_delay_ms = delay_ms;
    g_count = count;
    pthread_t t;
    pthread_create(&t, 0, pinger, 0);
    pthread_detach(t);
}
`;

function findCc(): string | null {
    for (const cc of ["cc", "clang", "gcc"]) {
        try {
            execFileSync(cc, ["--version"], { stdio: "ignore" });
            return cc;
        } catch {
            // try next
        }
    }
    return null;
}

function buildLibrary(): string | null {
    const cc = findCc();
    if (!cc || process.platform === "win32") {
        return null;
    }
    const dir = mkdtempSync(join(tmpdir(), "koffi-cb-repro-"));
    const src = join(dir, "pinger.c");
    const ext = process.platform === "darwin" ? "dylib" : "so";
    const lib = join(dir, `libpinger.${ext}`);
    writeFileSync(src, C_SOURCE);
    try {
        execFileSync(cc, ["-shared", "-fPIC", "-o", lib, src, "-lpthread"], { stdio: "ignore" });
    } catch {
        return null;
    }
    return existsSync(lib) ? lib : null;
}

describe("koffi foreign-thread callback delivery repro", () => {
    const libPath = buildLibrary();

    it.runIf(libPath)(
        "delivers background-thread callbacks while the main loop is idle",
        async () => {
            const lib = koffi.load(libPath!);
            const pingCb = koffi.pointer(koffi.proto("void ping_cb(int seq)"));
            const startPinger = lib.func("void start_pinger(ping_cb *cb, int delay_ms, int count)");

            const received: number[] = [];
            const receivedAt: number[] = [];
            const total = 5;

            // Keep-alive timer, exactly like FfiRuntimeHost: keeps the loop alive but the
            // main thread is otherwise IDLE between callbacks.
            const keepAlive = setInterval(() => {}, 4);

            const done = new Promise<void>((resolve) => {
                const cb = koffi.register((seq: number) => {
                    received.push(seq);
                    receivedAt.push(Date.now());
                    if (received.length >= total) {
                        resolve();
                    }
                }, pingCb);

                // Background thread pings every 500ms — long enough that the main loop
                // goes fully idle (only the keep-alive timer) between pings.
                startPinger(cb, 500, total);
            });

            const settled = await Promise.race([
                done.then(() => "done" as const),
                new Promise<"timeout">((r) => setTimeout(() => r("timeout"), 15_000)),
            ]);

            clearInterval(keepAlive);

            process.stderr.write(
                `[repro] received ${received.length}/${total} (${settled}); ` +
                    `gaps(ms)=${receivedAt.map((t, i) => (i ? t - receivedAt[i - 1] : 0)).join(",")}\n`
            );

            expect(settled).toBe("done");
            expect(received).toEqual([0, 1, 2, 3, 4]);
        },
        20_000
    );
});
