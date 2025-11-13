def hello_world_handler(event, context):
    print("Event received:", event)
    return {"statusCode": 200, "body": "Hello, World!"}
