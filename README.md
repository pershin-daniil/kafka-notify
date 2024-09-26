#### [kcat](https://github.com/edenhill/kcat)

```shell
brew install kcat jq
```

consume message

```shell
kcat -b localhost:9092 -t topic -C -o end -u | jq
```

produce message

```shell
echo '{"id":"b611c338-1e11-11ed-b5ce-461e464ebed9","chatId":"2d1bb2b4-1e11-11ed-9c9f-461e464ebed9","body":"Please help me!","fromClient":true}' | kcat -b localhost:9092 -t chat.messages -P
```

### [kafka](https://hub.docker.com/r/bitnami/kafka)

```shell
task up
```

```shell
task down
```