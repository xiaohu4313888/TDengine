const taos = require('../tdengine');
// const TAOS_BIND = require('../nodetaos/constants')
const ref = require('ref-napi');
const Struct = require('ref-struct-napi');
const taosConst = require('../nodetaos/constants');
const { Buffer } = require('buffer');

var conn = taos.connect({ host: "127.0.0.1", user: "root", password: "taosdata", config: "/etc/taos", port: 10 });
var cursor = conn.cursor();


var TAOS_BIND = Struct();
console.log("=======size======")
console.log(ref.sizeof.int);
console.error(ref.sizeof.void);
console.error(ref.sizeof.ulong);
console.error(ref.sizeof.int64);
console.error(ref.sizeof.uint);

console.error(ref.sizeof.pointer);
console.log("=======size======")
TAOS_BIND.defineProperty('buffer_type', ref.types.int);
TAOS_BIND.defineProperty('buffer', ref.refType(ref.types.void));
TAOS_BIND.defineProperty('buffer_length', ref.types.ulong);
TAOS_BIND.defineProperty('length', ref.refType(ref.types.ulong));
TAOS_BIND.defineProperty('is_null', ref.refType(ref.types.int));
TAOS_BIND.defineProperty('is_unsigned', ref.types.int);
TAOS_BIND.defineProperty('error', ref.refType(ref.types.int));
TAOS_BIND.defineProperty('u', ref.types.int64);
TAOS_BIND.defineProperty('allocated', ref.types.uint);

let intBind = new TAOS_BIND();


intBind.buffer = ref.alloc(ref.types.int,4);
// console.log(ref.address(intBind.buffer));
intBind.buffer_type = taosConst.getTypeCode("INT");
intBind.buffer_length = ref.sizeof.int;
intBind.length = ref.alloc(ref.types.ulong, ref.sizeof.int).ref();
intBind.is_null = ref.NULL;


let boolBind = new TAOS_BIND();

boolBind.buff = ref.alloc('bool',true);
// console.log(ref.address(boolBind.buff));
boolBind.buffer_type =  taosConst.getTypeCode("BOOLEAN");
console.log(boolBind.buffer_type )
boolBind.buffer_length = ref.sizeof.bool;
boolBind.length = ref.alloc(ref.types.ulong, ref.sizeof.bool).ref();
boolBind.is_null = ref.NULL;



let ncharBind = new TAOS_BIND();
let ncharBuf  = Buffer.alloc(ref.sizeof.pointer);
ref.set(ncharBuf,0,ref.allocCString("good"),'char *')
let str = "good";
let strLen = ref.sizeof.char * str.length;

ncharBind.buffer_type = taosConst.getTypeCode("NCHAR");
ncharBind.buff = ncharBuf.ref();

console.log(ref.isNull(ncharBind.buff));
ncharBind.buffer_length = ref.sizeof.char * str.length;
ncharBind.length = ref.alloc(ref.types.ulong,strLen);
ncharBind.is_null = ref.NULL;

// var bindsBuff = Buffer.alloc(TAOS_BIND.size * 1);
// bindsBuff.writePointer(intBind.ref(),0 *TAOS_BIND.size );
// // bindsBuff.writePointer(boolBind.ref(),1 *TAOS_BIND.size );
// bindsBuff.writePointer(ncharBind.ref(),2 *TAOS_BIND.size );

try {
    executeUpdate("drop database if exists nodedb;");
    executeUpdate("create database if not exists nodedb keep 36500;");
    executeUpdate("use nodedb;");
    executeUpdate("create table if not exists single_bind_para(ts timestamp, ii nchar(20))");
    // executeUpdate("create table if not exists single_bind_para(ts timestamp, ii int)");
    // executeUpdate("create table if not exists single_bind_para(ts timestamp, ii int,bl bool,nn nchar(100))");
    // executeUpdate("create table if not exists single_bind_para(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,"
    //     + "bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, "
    //     + "ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)");

    // let parepareSql = "insert into single_bind_para values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    let parepareSql = "insert into single_bind_para values (1,?)";
    cursor.stmtInit();
    cursor.stmtPrepare(parepareSql);
    cursor.bindParam(ncharBind);
    // console.log(ref.address(ncharBind));
    // cursor.bindParam(bindsBuff);
    cursor.addBatch();
    cursor.stmtExecute();
    cursor.closeStmt();
} catch (e) {
    console.log(e);
} finally {
    setTimeout(() => {
        conn.close()
    }, 2000);
}
function executeUpdate(sql) {
    console.log(sql);
    cursor.execute(sql);
}
