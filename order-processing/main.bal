import ballerinax/kafka;
import ballerina/io;

service  on new kafka:Listener([kafka:DEFAULT_URL,"localhost:9093","localhost:9094"],{topics:["pending-order"]}) {

    remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error? {
        // kafka:AnydataConsumerRecord[] x;

        // do {

        //     check Consumer -> poll(10);



        // } on fail var e {

        //  io:println(e.message());

        // }

        // io:println(x);

        // processes the records
        foreach var item in records {
                        
        }

        // commits the offsets manually
        kafka:Error? commitResult = caller->commit();
        if commitResult is kafka:Error {

            io:println("\n\nan error occured\n\n");

        }
    }

}

public function main() {
    io:println("Hello, World!");
}
