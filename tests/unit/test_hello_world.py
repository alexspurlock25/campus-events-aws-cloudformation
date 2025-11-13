from lib.hello_world import hello_world_handler


def test_hello_world_returns_200():
    result = hello_world_handler({}, None)

    assert result["statusCode"] == 200
    assert "Hello, World!" in result["body"]
