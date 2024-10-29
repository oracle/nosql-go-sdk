# Oracle NoSQL Database Go SDK: using native structs in put/get/query operations

The Oracle NoSQL go SDK supports directly writing and reading data to/from the NoSQL database using native Go structs. This bypasses having to convert data from structs into NoSQL MapValues and vice versa.

## Directly writing structs using PutRequest

`PutRequest` has an optional field called `StructValue`, which can be used instead of the normal `Value` field which requires a `types.MapValue`. To use this, set the `StructValue` field to a pointer to your native struct:
```
    sval := &MyStruct{...}
    putReq := &nosqldb.PutRequest{
        TableName:   tableName,
        StructValue: sval,
    }
```
The fields in the struct will be mapped to NoSQL row columns based on their field names, and optional annotations given in the struct fields. The annotation `nosql:"nnnn"` can be used to specify a database field name to map the struct field to, similar to the go `encoding/json` methods as documented for the [json Marshal function](https://pkg.go.dev/encoding/json#Marshal). If the struct already uses JSON annotations, those will be used if there are no additional `nosql` annotations:
```
    type MyStruct struct {
        // Will be written as column "Id"
        Id         int
        // Will be written as column "phone_number"
        Phone      string  `nosql:"phone_number"`
        // Json annotations can be used as well: will be written as column "userAge"
        Age        int   `json:"userAge"`
        // Timestamp values are supported: this will be written as column "StartDate"
        StartDate  time.Time
    }
```
Complex fields (maps, arrays) are supported, provided that their corresponding NoSQL columns are of the same type.

Note: only exported fields will be written. Any private fields (fields starting with a lowercase letter) will be ignored.

Native structs written to the NoSQL database in this way will be serialized directly to the internal NoSQL byte output stream, bypassing all `types.MapValue` processing. As such, using this method is more efficient, using less memory and CPU cycles.


## Directly reading structs using GetRequest

There are two methods for retrieving data from NoSQL into native structs:
1. Supply an allocated struct with primary key fields populated: on successful return, remaining fields in the struct will be filled in with row data
2. Specify the desired type of struct returned: the SDK will allocate a new struct of this type and populate its fields with row data

### Supplying an existing struct
```
    nval := &MyStruct{Id: 10}
    getReq := &nosqldb.GetRequest{
        TableName:   tableName,
        StructValue: nval,
    }
    getRes, err := client.Get(getReq)
    if err == nil {
        // nval now has all row data filled in
        // nval is also available via getRes.StructValue
    }
```

### Specifying desired type of allocated struct
```
import "reflect"


    nval := &MyStruct{Id: 10}
    getReq := &nosqldb.GetRequest{
        TableName:   tableName,
        StructValue: nval,
        StructType:  reflect.TypeOf((*MyStruct)(nil)).Elem(),
    }
    getRes, err := client.Get(getReq)
    if err == nil {
        // nval is left unchanged
        // getRes.StructValue contains a newly allocated populated struct
    }
```
It is not required to use a native struct for the primary key in the `GetRequest`. A MapValue can be used instead:
```
import "reflect"

    key := &types.MapValue{}
    key.Put("id", 10)
    getReq := &nosqldb.GetRequest{
        TableName: tableName,
        Key:       key,
        StructType:  reflect.TypeOf((*MyStruct)(nil)).Elem(),
    }
    getRes, err := client.Get(getReq)
    if err == nil {
        // getRes.StructValue contains a newly allocated populated struct
    }
```
Native structs read from the NoSQL database in this way will be deserialized directly from the internal NoSQL byte input stream, bypassing all `types.MapValue` processing. As such, using this method is more efficient, using less memory and CPU cycles.


## Reading native structs in queries

Query results can be internally converted to native structs by specifying `StructType` in `QueryRequest`, and using `GetStructResults`:

```
import "reflect"

    stmt = fmt.Sprintf("SELECT * FROM %s", tableName)
    queryReq := &nosqldb.QueryRequest{
        Statement:   stmt,
        StructType:  reflect.TypeOf((*MyStruct)(nil)).Elem(),
    }
    var allStructs []MyStruct
    for {
        queryRes, err := client.Query(queryReq)
        if err != nil {
            break
        }
        // retrieve slice of direct structs using GetStructResults:
        res, err := queryRes.GetStructResults()
        if err != nil {
            break
        }
        // res is a slice of `any`. Convert to MyStruct:
        for i := 0; i < len(res); i++ {
            if v, ok := res[i].(*MyStruct); ok {
                allStructs = append(allStructs, *v)
            }
        }
        if queryReq.IsDone() {
            break
        }
    }
```
Unlike Put and Get, query results will be internally converted from `types.MapValue` to native structs after all query processing is complete.

