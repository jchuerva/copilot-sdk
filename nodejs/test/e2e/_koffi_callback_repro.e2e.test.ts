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

// A side-effect-free call used to mimic the SDK's async FFI traffic (writes/pump)
// happening concurrently with background-thread callbacks.
int noop(void) { return 0; }

typedef void (*blob_cb)(int seq, const unsigned char *data, int len);

static blob_cb g_blob_cb;
static int g_burst;
static int g_big_at;
static int g_big_len;

static void *burster(void *arg) {
    (void)arg;
    // Small settle so the JS main loop is idle/parked before the burst starts.
    usleep(200 * 1000);
    static unsigned char buf[65536];
    for (int i = 0; i < g_burst; i++) {
        int len = (i == g_big_at) ? g_big_len : 64;
        for (int j = 0; j < len; j++) buf[j] = (unsigned char)(i + j);
        if (g_blob_cb) g_blob_cb(i, buf, len);
    }
    return 0;
}

// Blast 'burst' callbacks back-to-back (no delay), one of them 'big_len' bytes,
// mirroring a streaming turn's rapid inbound frames including a large payload.
void start_burster(blob_cb cb, int burst, int big_at, int big_len) {
    g_blob_cb = cb;
    g_burst = burst;
    g_big_at = big_at;
    g_big_len = big_len;
    pthread_t t;
    pthread_create(&t, 0, burster, 0);
    pthread_detach(t);
}

// Bidirectional reentrancy repro. A background "reader" thread invokes an inbound
// callback for each frame; the JS callback may synchronously call back into
// 'sink' (mimicking the SDK's synchronous connection_write from inside the koffi
// inbound callback). Because koffi relays foreign-thread callbacks with
// napi_tsfn_blocking, the reader thread is blocked in the callback while JS runs —
// and JS re-enters native via 'sink' on the main thread. This is the exact
// native->JS->native reentrancy the in-process transport performs under streaming.
typedef void (*inbound_cb)(int seq);

static inbound_cb g_inbound;
static int g_frames;
static volatile int g_sink_count;

// Called by JS from inside the inbound callback (like connection_write). Cheap.
void sink(int seq) { (void)seq; g_sink_count++; }

static void *reader(void *arg) {
    (void)arg;
    usleep(150 * 1000);
    for (int i = 0; i < g_frames; i++) {
        if (g_inbound) g_inbound(i); // blocks until the JS callback returns
    }
    return 0;
}

void start_reader(inbound_cb cb, int frames) {
    g_inbound = cb;
    g_frames = frames;
    g_sink_count = 0;
    pthread_t t;
    pthread_create(&t, 0, reader, 0);
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

    it.runIf(libPath)(
        "delivers a rapid burst of callbacks (incl. a large payload) after going idle",
        async () => {
            const lib = koffi.load(libPath!);
            const blobCb = koffi.pointer(
                koffi.proto("void blob_cb(int seq, uint8 *data, int len)")
            );
            const startBurster = lib.func(
                "void start_burster(blob_cb *cb, int burst, int big_at, int big_len)"
            );

            const burst = 40;
            const bigAt = 20;
            const bigLen = 23638; // same size as the streaming frame that preceded the stall
            const received: number[] = [];
            const lengths: number[] = [];

            // Keep-alive timer exactly like FfiRuntimeHost.
            const keepAlive = setInterval(() => {}, 4);

            const done = new Promise<void>((resolve) => {
                const cb = koffi.register((seq: number, dataPtr: unknown, len: number) => {
                    // Decode like feedInbound does, to exercise the same path.
                    koffi.decode(dataPtr, koffi.array("uint8", len, "Typed"));
                    received.push(seq);
                    lengths.push(len);
                    if (received.length >= burst) {
                        resolve();
                    }
                }, blobCb);
                startBurster(cb, burst, bigAt, bigLen);
            });

            const settled = await Promise.race([
                done.then(() => "done" as const),
                new Promise<"timeout">((r) => setTimeout(() => r("timeout"), 15_000)),
            ]);

            clearInterval(keepAlive);

            process.stderr.write(
                `[repro-burst] received ${received.length}/${burst} (${settled}); ` +
                    `bigLen=${lengths[bigAt] ?? "n/a"}\n`
            );

            expect(settled).toBe("done");
            expect(received.length).toBe(burst);
            expect(lengths[bigAt]).toBe(bigLen);
        },
        20_000
    );

    it.runIf(libPath)(
        "delivers frames when the JS callback re-enters native synchronously",
        async () => {
            const lib = koffi.load(libPath!);
            const inboundCb = koffi.pointer(koffi.proto("void inbound_cb(int seq)"));
            const startReader = lib.func("void start_reader(inbound_cb *cb, int frames)");
            // Synchronous native call issued from inside the inbound callback, mimicking
            // the SDK's synchronous connection_write from inside feedInbound.
            const sink = lib.func("void sink(int seq)");

            const frames = 60;
            const received: number[] = [];

            const keepAlive = setInterval(() => {}, 4);

            const done = new Promise<void>((resolve) => {
                const cb = koffi.register((seq: number) => {
                    // Re-enter native synchronously while this callback (running on, and
                    // blocking, the reader thread relay) is on the stack.
                    sink(seq);
                    received.push(seq);
                    if (received.length >= frames) {
                        resolve();
                    }
                }, inboundCb);
                startReader(cb, frames);
            });

            const settled = await Promise.race([
                done.then(() => "done" as const),
                new Promise<"timeout">((r) => setTimeout(() => r("timeout"), 15_000)),
            ]);

            clearInterval(keepAlive);

            process.stderr.write(
                `[repro-reentrant] received ${received.length}/${frames} (${settled})\n`
            );

            expect(settled).toBe("done");
            expect(received.length).toBe(frames);
        },
        20_000
    );
});
