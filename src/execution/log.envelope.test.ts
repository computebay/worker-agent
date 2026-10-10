import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { stampLogChunk } from "./log.envelope.js";

describe("stampLogChunk", () => {
  test("attaches the injected instant without touching the chunk", () => {
    const fixedNow = Date.parse("2026-08-09T09:28:00.000Z");
    const stamped = stampLogChunk("hello\nworld\n", fixedNow);
    assert.equal(stamped.ts, fixedNow);
    assert.equal(stamped.chunk, "hello\nworld\n");
  });

  test("defaults to Date.now() and stays an integer epoch-millis", () => {
    const before = Date.now();
    const stamped = stampLogChunk("x");
    const after = Date.now();
    assert.ok(Number.isInteger(stamped.ts));
    assert.ok(stamped.ts >= before && stamped.ts <= after);
  });

  test("same wall-clock instant from any producer timezone is identical", () => {
    // Epoch millis are absolute: a worker in +05:30 and one in -08:00
    // describing "the same moment" produce the same number.
    const instant = Date.parse("2026-08-09T09:28:08.614Z");
    assert.equal(stampLogChunk("a", instant).ts, stampLogChunk("b", instant).ts);
  });

  test("JSON round-trip (RabbitMQ payload) preserves the instant exactly", () => {
    const instant = Date.parse("2026-08-09T09:28:08.614Z");
    const stamped = stampLogChunk("out", instant);
    const revived = JSON.parse(JSON.stringify({ jobId: "job-1", ...stamped }));
    assert.equal(revived.ts, instant);
    assert.equal(typeof revived.ts, "number");
  });
});
