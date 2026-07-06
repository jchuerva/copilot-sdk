/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it, onTestFinished } from "vitest";
import { CopilotClient, RuntimeConnection } from "../../src/index.js";

function onTestFinishedForceStop(client: CopilotClient) {
    onTestFinished(async () => {
        try {
            await client.forceStop();
        } catch {
            // Ignore cleanup errors - process may already be stopped
        }
    });
}

describe("In-process FFI transport", () => {
    it("should start and connect over in-process FFI", async () => {
        // In-process FFI hosting resolves the CLI entrypoint (COPILOT_CLI_PATH or the
        // bundled platform package) and its sibling native runtime library itself. If
        // neither is available, start() throws and the test fails hard.
        const client = new CopilotClient({ connection: RuntimeConnection.forInProcess() });
        onTestFinishedForceStop(client);

        await client.start();

        const pong = await client.ping("ffi message");
        expect(pong.message).toBe("pong: ffi message");
        expect(Date.parse(pong.timestamp)).not.toBeNaN();

        expect(await client.stop()).toHaveLength(0); // No errors on stop
    });

    it("should resolve the in-process transport from COPILOT_SDK_DEFAULT_CONNECTION", async () => {
        // No explicit connection: the default is resolved from the env var.
        const client = new CopilotClient({
            env: { ...process.env, COPILOT_SDK_DEFAULT_CONNECTION: "inprocess" },
        });
        onTestFinishedForceStop(client);

        await client.start();

        const pong = await client.ping("env default");
        expect(pong.message).toBe("pong: env default");

        expect(await client.stop()).toHaveLength(0);
    });
});
