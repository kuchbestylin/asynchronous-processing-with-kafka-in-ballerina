import ballerina/io;
import ballerinax/kafka;
import ballerina/http;

public kafka:Consumer consumer_api_state_groceries = check new ([kafka:DEFAULT_URL],consumerConfigs);
public kafka:Consumer consumer_approved_order = check new ([kafka:DEFAULT_URL,"localhost:9093","localhost:9094"],{groupId:"approvals", topics:["approved-order"]});
public kafka:Producer producer_pending_order = check new([kafka:DEFAULT_URL,"localhost:9093","localhost:9094"], producerConfigs);
public type product record {|
    readonly int productID;
    string name;
    int quantity;
    float price;
    string description;
    string store;
|};
public type customer record {|
    readonly int customerID?;
    string name;
    readonly string email;
    string address;
    string cellphone;
    string password;
    string store?;
|};

public type oder record {
    int productID;
    int quantity;
};

public type customerOder record{
    int orderID;
    int customerID;
    oder[] d1oders;
    oder[] d2oders = [];
    oder[] d3oders = [];
};

public type approvedOrder record{
    int id;
    string value;
};

int orderIdentity = 0;
int customerCount = 0;
kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "pendings",
    autoCommit: false,
    offsetReset: "earliest",
    clientId: "consumer_api_state_groceries-pendings-1" 
};

kafka:ProducerConfiguration producerConfigs = {
    acks: "all"
};

table<product> key(productID) groceriesd1 = table[
    {productID: 0,name: "ps4",price: 450,description: "gaming console by playstation",quantity: 44,store: "d1"}
];
table<product> key(productID) groceriesd2 = table[];
table<product> key(productID) groceriesd3 = table[];
table<customer> key(email) customers = table[];

// service  on new kafka:Listener(kafka:DEFAULT_URL,{topics:["approved"]}) {

//     remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error? {
//         worker consumerRes {
//             json value = ();
//             foreach var item in records {
//                 string x = (check string:fromBytes(<byte[]>item.value));
//                 io:println(x);
//                 if x.toJson().store == "d2"{
//                     value = x.toJson();
//                 }

//             } on fail var e {
//             	io:println(e.message());
//             }       
//         }
//     }

// }

service /choppies on new http:Listener(8080) {
    resource function get d1/stock () returns json {
        return groceriesd1.toArray().toJson();    
    }
    resource function get d2/stock () returns json {
        return groceriesd1.toArray().toJson();    
    }
    resource function get d3/stock () returns json {
        return groceriesd1.toArray().toJson();    
    }

    resource function post d1/register (@http:Payload customer payload) returns json|error {
        boolean exists = customers.hasKey(payload.email);
        if exists is true{
            return {"value":-1,"message":"User already registered"};
        }
        customer prospective = {customerID: customerCount + 1,address: payload.address, cellphone: payload.cellphone, email: payload.email,name: payload.name, password: payload.password,store: "d1"};
        _ = check producer_pending_order -> send({value:prospective, topic:"new-user"});
        return {"value":prospective.customerID, "message": "your customer id is: "+customerCount.toString()};
    }

    resource function post d1/[int customerID]/purchase (@http:Payload oder[] oders) returns json|error {
        json r_v = ();
        oder[] d2Oder= [];
        oder[] d3Oder = [];
        oder[] toBeRequested = [];
        boolean cantBeSatisfied = false;
        foreach var item in oders {
            if item.productID < 0 || item.quantity < 1{
                return 0;
            }
            if(groceriesd1.hasKey(item.productID)) == false{
                return{"value":0,"message":"product with id=" + item.productID.toString() + " does not exist"};
            }
        }
        int counter = 0;
        foreach var item in oders {
            int available = groceriesd1.get(item.productID).quantity;
            if available < item.quantity{
                int needed = item.quantity - available;
                if(needed > 0){
                    toBeRequested.push({productID: item.productID, quantity: needed});
                    oders[counter].quantity = item.quantity - needed;
                }
            }
            counter += 1;

        }
        foreach var item in toBeRequested {
            int toDispatch = item.quantity;
            int d2Units = groceriesd2.get(item.productID).quantity;
            int d3Units = groceriesd3.get(item.productID).quantity;
                if(d2Units >= item.quantity){
                d2Oder.push({productID: item.productID, quantity: item.quantity});
                }
                else if(d3Units >= item.quantity){
                d3Oder.push({productID: item.productID, quantity: item.quantity});
                }
                else if((d3Units+d2Units) > toDispatch){
                d2Oder.push({productID: item.productID, quantity: toDispatch-d3Units});
                d3Oder.push({productID: item.productID, quantity: toDispatch-d2Units});
                }
                else {
                    cantBeSatisfied = true;
                }

        }
        customerOder request = {orderID: orderIdentity + 1, customerID: customerID, d1oders: oders};
        orderIdentity += 1;

        if d2Oder.length() > 0{
            request.d2oders = d2Oder;
        }
        if d3Oder.length() > 0{
            request.d3oders = d3Oder;
        }
        if cantBeSatisfied == false {
            kafka:Error? send = producer_pending_order -> send({value:request.toString().toBytes(), topic: "pending-order"});
            if send is kafka:Error{
                io:println("an Error occured: Publishing a d1 oder");
                return{"value":-1,"message":send.message()};
            }
        }else {
            return {"value":-1, "message": "request cannot be satisfied. Not enough product units"};
        }
        int x = 0;
        do {
            _ = check consumer_approved_order -> subscribe(["approved-order"]);
            while x < 100 {
                kafka:AnydataConsumerRecord[] records = check consumer_approved_order -> poll(2);
                foreach var item in records {
                    byte[] marshalled = <byte[]>item.value;
                    int i = check int:fromString(check string:fromBytes(<byte[]>marshalled));
                    if(i == request.orderID){
                        r_v = {"id":i,"status":"approved"};
                        x = 200;
                    }
                }
                x+=1;
            }
        } 
        on fail var e {
            io:println(e.message());
        }
        if(r_v.status == "approved"){
            return {"value":1, "message":"Oder Approved!!!. Products will be delivered in the next hour. Please follow the <----link----> to see your email "};
        }else{
            return {"value":0, "message":"Oder has not been approved!"};
        }
    }
    resource function post d2/[int customerID]/purchase (@http:Payload oder[] oders) returns json {
        foreach var item in oders {
            if item.productID == 0 || item.quantity < 1{
                return 0;
            }
        }
        kafka:Error? send = producer_pending_order -> send({value:oders.toString().toBytes(), topic: "oders-d2"});
        if send is kafka:Error{
            io:println("an Error occured: Publishing a d2 oder");
        }

    }
    resource function post d3/[int customerID]/purchase (@http:Payload oder[] oders) returns json {
        foreach var item in oders {
            if item.productID == 0 || item.quantity < 1{
                return 0;
            }
        }
        kafka:Error? send = producer_pending_order -> send({value:oders.toString().toBytes(), topic: "oders-d3"});
        if send is kafka:Error{
            io:println("an Error occured: Publishing a d3 oder");
        }

    }

}





// function loadRecords(string topic) returns error? {
//      do {
// 	     _ = check consumer_api_state_groceries -> subscribe([topic]);
//         while true {
//             kafka:AnydataConsumerRecord[] records = check consumer_api_state_groceries -> poll(2);
//             foreach var item in records {
//                 byte[] marshalled = <byte[]>item.value;
//                 json product = check string:fromBytes(marshalled);
//                 io:println(product);
//             }
//         }
//      } on fail var e {
//      	io:println(e.message());
//      }
// }

// public function main() returns error? {
//     _ = check loadRecords("cities");
// }