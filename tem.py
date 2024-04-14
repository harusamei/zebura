import tensorflow_hub as hub

# Load the Universal Sentence Encoder's TF Hub module
model = hub.load("https://tfhub.dev/google/universal-sentence-encoder/4")

# Compute a representation for each message
messages = ["Hello, world!", "The quick brown fox jumps over the lazy dog."]
message_embeddings = model(messages)

for i, message_embedding in enumerate(message_embeddings):
    print(f"Message: {messages[i]}")
    print(f"Embedding: {message_embedding.numpy()}")