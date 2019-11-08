var thrift = require("thrift");
var ThriftHiveMetastore = require("./gen-nodejs/ThriftHiveMetastore.js");
var kerberos = require('kerberos');
const mechOID = kerberos.GSS_MECH_OID_KRB5;

var HiveMetaStoreClient = function (host, port, options) {
    var connection = thrift.createHttpConnection(host, port, options);
    this.client = thrift.createHttpClient(ThriftHiveMetastore, connection);
}

HiveMetaStoreClient.prototype.get_all_databases = function (callback) {
    return this.client.get_all_databases(callback);
}

HiveMetaStoreClient.prototype.get_databases = function (pattern, callback) {
    return this.client.get_databases(pattern, callback);
}

HiveMetaStoreClient.prototype.get_database = function (dbName, callback) {
    return this.client.get_database(dbName, callback);
}

HiveMetaStoreClient.prototype.get_all_tables = function (dbName, callback) {
    return this.client.get_all_tables(dbName, callback);
}

HiveMetaStoreClient.prototype.get_tables_by_type = function (dbName, pattern, tblType, callback) {
    return this.client.get_tables_by_type(dbName, pattern, tblType, callback);
}

HiveMetaStoreClient.prototype.get_table = function (dbName, tblName, callback) {
    return this.client.get_table(dbName, tblName, callback);
}

HiveMetaStoreClient.prototype.get_partition_names = function (dbName, tblName, max_parts, callback) {
    return this.client.get_partition_names(dbName, tblName, max_parts, callback);
}

HiveMetaStoreClient.prototype.get_partitions = function (dbName, tblName, max_parts, callback) {
    return this.client.get_partitions(dbName, tblName, max_parts, callback);
}

async function getKerberosToken(hostname) {
    let service = 'HTTP/' + hostname;
    if(process.platform == 'linux'){
       service = 'HTTP@' + hostname;
    }
    let kerberosClient = await kerberos.initializeClient(service, { mechOID });
    let kerberosToken = await kerberosClient.step('');
    return kerberosToken;
}

module.exports.DefaultOptions = {
    transport: thrift.TBufferedTransport,
    protocol: thrift.TJSONProtocol,
    path: "/gateway/default/hmshttpthrift/api/hms",
    headers: { "Connection": "close" },
    https: true
};

module.exports.CreateClient = async function (host, port, authType, params, callback) {
    if (!host || !port || !authType){
        throw new Error('Input required for fields: {host, port, authType}');
    }
    let options = params.options || this.DefaultOptions;
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
    if (authType == 'basic'){
        if (!params.username || !params.password){
            throw new Error('Input required for fields:{username, password} for HiveMetaStore API when using basic authTpye');
        }
        let auth = 'Basic ' + Buffer.from(params.username + ':' + params.password).toString('base64');
        options.headers.Authorization = auth;
    } else if(authType == 'kerberos'){
        let kerberosToken = await getKerberosToken(host);
        options.headers.Authorization = `Negotiate ${kerberosToken}`;
    }
    let client = new HiveMetaStoreClient(host, port, options);
    if (typeof callback === 'function'){
        callback(client)
    }else{
        return client
    }
}
