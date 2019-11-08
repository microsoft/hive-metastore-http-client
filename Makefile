genthrift:
	rm -rf ./src/gen-nodejs/*
	mkdir -p ./src/gen-nodejs
	thrift -r --gen js:node -out ./src/gen-nodejs ./thrift/hive_metastore.thrift
	npm install -g dts-gen
	npm install -g ./src/
	dts-gen -m hive-metastore-http-client -f src/hive-metastore-http-client.d.ts
