# SO3

## Configuration

`so3` now supports configuration from a TOML file in addition to environment variables.

- By default the binary looks for `./so3.toml`.
- `SO3_CONFIG` can point to a different TOML file.
- Environment variables still override TOML values.

Example:

```toml
node_id = "123e4567-e89b-12d3-a456-426614174000"
object_api_addr = "127.0.0.1:3000"
rpc_api_addr = "127.0.0.1:4000"
object_request_timeout_secs = 10
data_dir = "./var/so3"

[cluster]
peers = ["127.0.0.1:4001", "127.0.0.1:4002"]
```

## License

All code in this repository is dual-licensed under either:

* MIT License ([LICENSE-MIT](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))
* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)
  or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))

at your option.
