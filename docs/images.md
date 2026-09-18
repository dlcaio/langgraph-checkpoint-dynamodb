# Image references in graph state

Store image files in application-managed storage, such as S3, and keep stable references in LangGraph state. LangChain's [checkpointing guidance](https://kb.langchain.com/articles/1242226068-how-do-i-configure-checkpointing-in-langgraph) recommends this approach for large payloads.

The flow is:

```text
Application or tool uploads image → state receives stable reference
Model node resolves reference → model receives image → state receives answer
Checkpointer saves reference and answer
```

## Example

This example uses an application-provided image storage adapter and a vision-capable LangChain chat model. Implement `ImageStore` using your chosen storage service; the interface below is an example application contract, not an export from this package. `put` must finish the upload before returning an immutable reference. `getReadUrl` must authorize access and return an HTTPS URL that the model provider can fetch, valid for the duration of the request.

```ts
import { randomUUID } from 'node:crypto';
import { HumanMessage } from '@langchain/core/messages';
import type { BaseChatModel } from '@langchain/core/language_models/chat_models';
import { Annotation, END, START, StateGraph } from '@langchain/langgraph';
import { DynamoDBSaver } from '@dlcaio/langgraph-checkpoint-dynamodb';

interface ImageReference {
  key: string;
  mimeType: string;
}

interface ImageStore {
  put(bytes: Uint8Array, mimeType: string): Promise<ImageReference>;
  getReadUrl(image: ImageReference): Promise<string>;
}

// Supplied by your application; these are not stored in graph state.
declare const images: ImageStore;
declare const model: BaseChatModel;
declare const uploadedImage: Uint8Array;

const State = Annotation.Root({
  image: Annotation<ImageReference>,
  question: Annotation<string>,
  answer: Annotation<string>,
});

const saver = new DynamoDBSaver({
  tableName: 'agent-checkpoints',
  clientConfig: { region: 'eu-west-1' },
  retentionSeconds: 7 * 24 * 60 * 60,
});

const graph = new StateGraph(State)
  .addNode('describe', async (state) => {
    // Resolve again on each execution so resume does not depend on an old URL.
    const url = await images.getReadUrl(state.image);
    const response = await model.invoke([
      new HumanMessage({
        content: [
          { type: 'text', text: state.question },
          { type: 'image', url, mimeType: state.image.mimeType },
        ],
      }),
    ]);
    // The temporary image message is not returned as a state update.
    return { answer: response.text };
  })
  .addEdge(START, 'describe')
  .addEdge('describe', END)
  .compile({ checkpointer: saver });

try {
  // Upload before graph.invoke so the input checkpoint contains only a reference.
  const image = await images.put(uploadedImage, 'image/png');
  const result = await graph.invoke(
    { image, question: 'Describe this image.' },
    { configurable: { thread_id: randomUUID() } },
  );
  console.log(result.answer);
} finally {
  saver.destroy();
}
```

Use a model integration that supports URL image inputs. LangChain documents [image content blocks](https://docs.langchain.com/oss/javascript/langchain/messages); supported inputs vary by provider. If your provider requires base64 or a file ID, resolve the reference to that format inside the node instead. Keep temporary image bytes out of returned state updates and persisted task outputs. Model-call tracing has its own data retention policy; this pattern only controls checkpoint contents.

For a conversation graph, retain image references alongside the relevant messages and resolve the images needed for each model call. Putting bytes in `ToolMessage.artifact` does not externalize them: if the message is checkpointed, its artifact can be serialized too.

## External images and checkpoint overflow have separate lifecycles

The saver's `s3` option is optional overflow storage for serialized checkpoint payloads. It does not implement `ImageStore`, extract embedded images, or fetch referenced files. For example, if a messages channel containing embedded images exceeds the configured threshold, the saver can offload that entire serialized channel value; it does not deduplicate individual images within it.

Use a separate bucket or non-overlapping prefix for application images. Saver cleanup removes its whole thread object prefix. It does not resolve image references, and `deleteThread` or `sweepExpired` will not delete separately stored images.

Your application must retain external images while any supported resume or replay still needs them, including references from other threads. Keep a stable object key or artifact ID in state, not an expiring signed URL as the sole reference. If an upload succeeds but the workflow never saves its reference, your application's cleanup policy must handle that unused image too.
