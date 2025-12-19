import asyncio

from crawler.parser import HTMLParser


async def test_valid_html_parse():
    html = """
    <html>
      <head>
        <title>Test Page</title>
        <meta name="description" content="desc">
        <meta name="keywords" content="a,b,c">
      </head>
      <body>
        <h1>Main</h1>
        <h2>Sub</h2>
        <p>Hello <a href="/path">world</a></p>
        <img src="/img.png" alt="logo">
        <ul><li>one</li><li>two</li></ul>
        <table><tr><th>h</th></tr><tr><td>v</td></tr></table>
      </body>
    </html>
    """
    parser = HTMLParser()
    result = await parser.parse_html(html, "https://example.com/base")

    assert result["title"] == "Test Page"
    assert result["metadata"]["description"] == "desc"
    assert result["metadata"]["keywords"] == "a,b,c"
    assert "https://example.com/path" in result["links"]
    assert len(result["images"]) == 1
    assert result["images"][0]["src"] == "https://example.com/img.png"
    assert result["headings"]["h1"] == ["Main"]
    assert result["headings"]["h2"] == ["Sub"]
    assert result["tables"][0][0] == ["h"]
    assert result["tables"][0][1] == ["v"]
    assert result["lists"]["ul"][0] == ["one", "two"]


async def test_broken_html_returns_partial():
    html = "<html><head><title>Oops"                  
    parser = HTMLParser()
    result = await parser.parse_html(html, "https://example.com")
                                                    
    assert result["url"] == "https://example.com"
    assert "error" not in result
    assert result["title"] == "Oops"


async def test_relative_links_converted_and_validated():
    html = """
    <a href="/a"></a>
    <a href="https://external.com/x"></a>
    <a href="#anchor"></a>
    <a href="javascript:void(0)"></a>
    <a href="mailto:test@example.com"></a>
    """
    parser = HTMLParser()
    result = await parser.parse_html(html, "https://example.com/base")
    links = result["links"]

    assert "https://example.com/a" in links
    assert "https://external.com/x" in links
    assert not any("#" in link for link in links)
    assert not any("javascript:" in link for link in links)
    assert not any(link.startswith("mailto:") for link in links)


async def test_images_with_alt_and_absolute_src():
    html = """
    <img src="/img1.png" alt="one">
    <img src="https://cdn.com/img2.png" alt="two">
    <img src="" alt="skip">
    """
    parser = HTMLParser()
    result = await parser.parse_html(html, "https://example.com")
    images = result["images"]

    assert {"src": "https://example.com/img1.png", "alt": "one"} in images
    assert {"src": "https://cdn.com/img2.png", "alt": "two"} in images
                             
    assert all(img["src"] for img in images)


async def main():
    await test_valid_html_parse()
    await test_broken_html_returns_partial()
    await test_relative_links_converted_and_validated()
    await test_images_with_alt_and_absolute_src()
    print("Day 2 tests passed")


if __name__ == "__main__":
    asyncio.run(main())