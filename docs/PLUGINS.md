# Writing Custom Processor Plugins

This guide explains how to write and register new custom job processors in the distributed processing engine.

---

## 1. The `JobProcessor` Protocol

Every processor implements the `JobProcessor` protocol defined in `src/distributed_processing/processors/base.py`:

```python
from typing import Any, Protocol
from distributed_processing.processors.base import JobContext

class JobProcessor(Protocol):
    async def process(self, payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        """Execute the job transformation and return a result dictionary."""
        ...
```

### The `JobContext` Object

The runner passes a `JobContext` instance to each processor containing:
- `ctx.job_id`: Unique UUID of the job.
- `ctx.worker_id`: Worker hostname and process ID.
- `ctx.cfg`: Global `Settings` instance.
- `ctx.s3`: `S3Storage` client with `put_object()` and `get_object()`.
- `ctx.http_client`: `httpx.AsyncClient` for fetching external assets.

---

## 2. Example: Writing a Text Summarization Processor

Create `src/distributed_processing/processors/text_summary.py`:

```python
from typing import Any
from distributed_processing.processors.base import JobContext

class TextSummaryProcessor:
    """Computes word frequency and summary statistics for text payloads."""

    async def process(self, payload: dict[str, Any], ctx: JobContext) -> dict[str, Any]:
        text = str(payload.get("text", ""))
        words = text.split()
        
        # Calculate basic summary
        word_count = len(words)
        unique_words = len(set(w.lower() for w in words))
        
        # Save full analysis to S3
        analysis_key = f"analysis/{ctx.job_id}_summary.json"
        analysis_data = {
            "word_count": word_count,
            "unique_words": unique_words,
            "sample": words[:10],
        }
        await ctx.s3.put_object(
            ctx.cfg.s3_bucket_data,
            analysis_key,
            json.dumps(analysis_data).encode("utf-8"),
            content_type="application/json",
        )

        return {
            "word_count": word_count,
            "unique_words": unique_words,
            "artifact_s3_url": f"s3://{ctx.cfg.s3_bucket_data}/{analysis_key}",
        }
```

---

## 3. Registering the Processor

Open `src/distributed_processing/processors/registry.py` and register the new plugin in `create_default_registry()`:

```python
from distributed_processing.processors.text_summary import TextSummaryProcessor

def create_default_registry() -> ProcessorRegistry:
    reg = ProcessorRegistry()
    reg.register("image:blur", ImageBlurProcessor())
    reg.register("image:grayscale", ImageGrayscaleProcessor())
    reg.register("image:resize", ImageResizeProcessor())
    reg.register("data:transform", DataTransformProcessor())
    
    # Register your new processor
    reg.register("text:summary", TextSummaryProcessor())
    return reg
```

---

## 4. Submitting a Task to the New Processor

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "text:summary",
    "payload": {
      "text": "Distributed systems scale horizontally by decoupling producers and consumers using message queues."
    }
  }'
```
