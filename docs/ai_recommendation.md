# NadoBro AI Recommendation

## Current State

NadoBro currently uses:

- xAI and OpenAI through the `openai` Python SDK
- `src/nadobro/services/knowledge_service.py` for support and conversational answers
- `src/nadobro/services/bro_llm.py` for structured strategy decisions
- a static knowledge file plus lightweight keyword retrieval, not a vector index

## Recommendation

Do not add Gemini in the current repair pass.

Instead:

1. Keep the current xAI/OpenAI setup stable while the bot reliability and UX issues are being fixed.
2. Add a small provider abstraction layer before introducing any new model vendor.
3. Re-evaluate Gemini only after response quality, latency, and support costs are measured against the current production prompts.

## Why Gemini Should Wait

- The current code is organized around one SDK pattern and two providers. Adding Gemini now increases branching and testing cost during a stability-focused release.
- The larger reliability wins right now come from fixing handler consistency, event-loop blocking, and process shutdown behavior rather than changing LLM vendors.
- NadoBro's current support layer is limited more by prompt structure, retrieval quality, and conversation-state design than by the lack of a third provider.

## When Gemini Would Make Sense

Gemini becomes worth adding if one or more of these are true:

- conversational quality is consistently better for short, natural back-and-forth chats
- latency or cost is materially better for support-style traffic
- multimodal roadmap items become important, especially voice and audio
- the team wants provider diversity behind a unified AI interface

## Suggested Architecture Before Any Provider Expansion

Add a thin internal AI gateway with:

- one config surface for provider selection and model names
- one normalized request/response shape for chat completions
- one place for prompt templates, retry policy, and observability
- separate adapters for support chat and strategy decisioning so structured trading logic does not inherit support-chat assumptions

## Voice Mode Recommendation

Voice mode is promising, but it should be a separate feature phase rather than part of the current repair pass.

Recommended MVP shape:

1. Accept Telegram voice notes.
2. Transcribe audio to text.
3. Route the transcript through the existing support/conversation layer.
4. Return text first, not text-to-speech.
5. Capture explicit user feedback on whether the answer was helpful.

This sequence keeps cost, privacy, moderation, and debugging complexity much lower than shipping full duplex voice immediately.

## About "Training" NadoBro From User Inputs

Do not frame this as direct live training.

Safer and more realistic improvements are:

- save opted-in feedback signals such as helpful or not helpful
- log bad-answer examples for prompt and retrieval tuning
- build eval sets from recurring support questions
- improve retrieval and response policies before considering fine-tuning

## Next Step If Gemini Is Revisited

If the team decides to test Gemini later, the first implementation should be:

- provider abstraction first
- Gemini support only in the support-chat path
- no changes to `bro_llm.py` until the support path proves value
- side-by-side prompt and latency evaluation against the current stack
