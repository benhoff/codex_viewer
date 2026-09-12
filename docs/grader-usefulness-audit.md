# Grader usefulness audit — 2026-09-12

## Conclusion

The saved results are useful as limited descriptions of individual evidence slices. They do not yet support reliable whole-task quality judgments or model-selection decisions. The immediate problems are evidence representation, fragmentation, and missing synthesis. These runs do not establish that the loaded model needs a larger context window or lacks grading ability.

This audit inspected saved inputs/results, the original conversation and tool evidence, one decoded screenshot, current implementation, and endpoint metadata. It submitted no new grading requests and changed no settings. Counts below describe the inspection snapshot; run 4 was still marked running.

## Recent runs

| Run | Selected turns | Planned batches | Completed batches at inspection | Result |
| --- | --- | ---: | ---: | --- |
| 1 | 1–9 | 93 | 0 | Failed on the old 120-second timeout |
| 2 | 1–5 | 58 | 4 | Failed on an inconsistent rating/confidence response |
| 3 | 2–5 | 50 | 2 | Cancelled; used the older context format |
| 4 | 1–2 | 93 | 3 | Latest task-context format; still marked running |

Runs 1–3 concern session `01a091fd-f2ab-73c1-a5d9-08ed2d129147`. Run 4 concerns `01a0945a-f1e7-7a63-9be8-749ad466f68b`: a request to assess project-page clutter, followed by a clarification that scanning turns and opening their sessions is the critical workflow.

All nine completed batch grades across runs 2–4 reported an unknown outcome. That is often correct for an isolated early slice, but offers little help with deciding whether the finished work satisfied the user.

Run 4's first three grades assigned required capability levels 3, 2, and 3, respectively. They recognized the user request and described discovery/source inspection. They did not evaluate the completed UI critique. Those varying slice ratings should not be averaged into a task rating or treated as evidence that a particular model is sufficient.

## Evidence representation is wasting most of this run

Two tool-result events contain mixed text/image arrays. The normalizer only unwraps arrays composed entirely of text blocks. Mixed arrays survive as serialized text, including their base64 image data. The request then sends the evidence JSON as a text message; it does not turn these strings into image content parts.

* Event 68 contains 1,165,906 characters and spans batches 20–84.
* Event 93 contains 127,402 characters and spans batches 85–92.
* Consequently, 73 of 93 batches contain image-bearing events. Some boundary batches also contain useful text.
* The three image data URLs total 1,292,870 characters. All serialized batch payloads together occupy 1,662,527 bytes, including repeated context/envelopes. Encoded image data accounts for approximately 78% of that payload.
* An offline size experiment replacing only the image data URLs with short markers reduced the same planner's output to 21 batches and 300,615 bytes. This is a diagnostic comparison, not a proposed way to discard visual evidence from a UI assessment.

There is also residual operational boilerplate and repeated command output inside nested transport wrappers. The first batch includes a 10,888-character skills instruction record, alongside other operational instructions. Some later batches repeat source output already captured in another event.

The final UI critique (event 98) is not available until batch 92. The user's workflow clarification (105) and the agent's revised recommendation (108) appear in batch 93. Most of the expense therefore precedes access to the actual work product.

Relevant implementation: `evidence_text`/`clean_events` in `agent_operations_viewer/llm_grader.py`, and text serialization in `call_grader`.

## Context capacity versus usable context

The endpoint advertised a 32,768-token active context for `qwen3.8-27b-ud-q4_k_m`. The first three completed calls in run 4 used 5,124, 4,947, and 5,522 prompt tokens. Their outputs used 221, 299, and 252 tokens, all ending normally. Their durations were approximately 86, 82, and 93 seconds.

Those prompts use about 16% of the context window. The current byte-based upper bound and 20,000-character setting prevent overflow conservatively, but substantially underuse the available context for ordinary text. The observed outputs did not hit the 512-token cap. A larger final assessment may need a larger output allowance, but output truncation does not explain these three limited judgments.

The meaningful user and assistant conversation for this task totals only 6,449 characters, excluding the environment wrapper. A coherent package containing that conversation, targeted source evidence, measured browser checks, and correctly attached screenshots is a much more promising grading input than dozens of independent fragments.

Use the serving tokenizer and chat template to measure text requests, while accounting separately for images, schema overhead, output allowance, and reserve. The official llama.cpp server documentation describes `/tokenize`, `/apply-template`, and typed `image_url` message content: <https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md>. The endpoint advertises image input, but a real multimodal grading request still needs verification.

## Whole-task synthesis is absent

After completing the batch loop, `grade()` explicitly constructs an overall result with null capability levels, unknown confidence/outcome, and no findings. Its verification note asks a human to combine the batches. There is no final synthesis call.

This means even a successful 93-batch run cannot provide the whole-task assessment the user expects. Adding the original request to each slice improved local interpretation, but did not solve this architectural gap. Earlier tests demonstrated request completion, validation, and context availability; they did not establish grading usefulness or accuracy.

## What a useful assessment should cover for this example

The original work is an assessment and design recommendation, not an implementation request. A useful evaluator should check whether it:

1. Explains clutter using concrete observations, including competing turn/session columns, repeated navigation actions, metadata competing with controls, and excessive visual grouping.
2. Supports its mobile-layout claim. Browser measurements report a 3.5-pixel prompt container at a 390-pixel viewport; the saved screenshot corroborates awkward wrapping and crowding. These checks used sample data.
3. Prioritizes changes and explains their relation to the user's workflow.
4. Incorporates the user's clarification into a full-width turn timeline with drill-down into the relevant session.
5. Distinguishes observed problems from proposals that still require implementation and user validation.

The final responses contain substantial evidence for these criteria. A provisional human reading is that the work addressed the request with grounded recommendations, subject to the sample-data limitation and lack of a usability test. The grader should investigate that conclusion and cite the supporting evidence; it should not penalize the absence of code changes that were never requested.

## Recommended implementation sequence

1. **Repair media and transport normalization.** Extract actual tool text recursively from known wrappers, deduplicate mirrored records, and keep relevant instructions. Route image content as images or explicitly mark unavailable visual evidence. Never split encoded image bytes into text grading batches. Preserve source references and original records for inspection.
2. **Build a coherent task package.** Include the complete meaningful user dialogue, corrections, final responses, and relevant instructions. Add targeted patches, source excerpts, measurements, and screenshots according to the task type. Prioritize the work product and evidence needed to check it.
3. **Budget actual tokens.** Measure with the serving model and reserve output/template/media space within 32K. Test larger coherent text inputs on this endpoint; do not assume that increasing context is free in latency or that raising a character limit alone solves fit.
4. **Use extraction and synthesis for oversized tasks.** Have intermediate batches extract cited observations and unresolved questions. A final evaluator should receive the task dialogue, final deliverable, and accumulated evidence, with access to supporting excerpts. It must produce a whole-task judgment rather than average ordinal slice scores.
5. **Validate usefulness against human judgments.** Compare this example and small known good, incomplete, and flawed examples. Check task completion, evidence support, handling of corrections, citation accuracy, omissions, and runtime. Keep observed work quality separate from claims about the minimum model capable of doing it; model sufficiency needs comparative task trials.

Continuing run 4 in its current form is unlikely to be a useful use of inference time. Cancelling and resubmitting after the evidence and synthesis fixes is preferable to increasing its timeout or input-character setting.
