{
    "name": "SSE Server",
    "author": "justpickles00",
    "version": "1.0.0",
    "license": "LGPL-3",
    "depends": ["web"],
    "external_dependencies": {"python": ["aiohttp"]},
    "post_load": "post_load",
    "assets": {"web.assets_backend": ["sse_server/static/src/client.js"]},
}
