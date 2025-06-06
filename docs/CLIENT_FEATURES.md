# RedBase Client Features

RedBase now includes several advanced client features to enhance its usability and performance:

1. **Asynchronous API**: Interact with the database without blocking the current thread, which is particularly useful for applications that need to handle multiple concurrent requests.

2. **Batch Operations**: Perform multiple operations in a single transaction, which is more efficient than performing them one by one.

3. **Connection Pooling**: Efficiently reuse connections to the database, which is important for performance in multi-user scenarios.

4. **REST Interface**: Interact with the database over HTTP, which is useful for web applications and microservices.

For detailed documentation and examples of how to use these features, please see the [ADVANCED_USAGE.md](../ADVANCED_USAGE.md) file in the root directory of the project.