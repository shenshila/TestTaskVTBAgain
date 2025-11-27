import { Writer } from "k6/x/kafka";

const writer = new Writer({
  brokers: ["localhost:29092"],
  topic: "input-topic",
});

export default function () {
  // Максимально простой вариант
  const result = writer.produce({
    messages: [
      {
        value: "test"
      }
    ],
  });
  
  console.log("Success, offset:", result.offset);
}

export function teardown() {
  writer.close();
}

// --------------------------------------------
// import { Writer } from "k6/x/kafka";

// const writer = new Writer({
//   brokers: ["localhost:9092"],
//   topic: "input-topic",
//   autoCreateTopic: true,
// });

// // Функция для кодирования в base64
// function toBase64(str) {
//   return btoa(unescape(encodeURIComponent(str)));
// }

// export default function () {
//   try {
//     const messageData = {
//       message: "Test message from k6",
//       timestamp: new Date().toISOString(),
//       virtualUser: __VU,
//       iteration: __ITER
//     };
    
//     const result = writer.produce({
//       messages: [
//         {
//           key: toBase64(`key-${__VU}-${__ITER}`),
//           value: toBase64(JSON.stringify(messageData))
//         }
//       ],
//     });
    
//     console.log(`✅ Message sent successfully. Offset: ${result.offset}`);
//   } catch (error) {
//     console.error(`❌ Failed to send message: ${error}`);
//   }
// }

// export function teardown() {
//   writer.close();
// }

// import { Kafka, KafkaClient, Writer, Producer } from "k6/x/kafka";

// console.log("Testing Kafka extension API...");

// // Проверяем, какие классы доступны
// console.log("Kafka:", typeof Kafka);
// console.log("KafkaClient:", typeof KafkaClient); 
// console.log("Writer:", typeof Writer);
// console.log("Producer:", typeof Producer);

// export default function () {
//   console.log("Kafka test completed");
// }


// ---------------------------------------------------
// import { KafkaClient, Producer } from "k6/x/kafka";

// export default function () {
//     console.log("Kafka extension is working!");
// }



// --------------------------------------------
// import { KafkaClient, Producer } from "k6/x/kafka";

// export default function () {
//     console.log("Kafka extension is working!");
// }

// -----------------------------------------
// import { Kafka } from 'k6/x/kafka';

// const kafka = new Kafka({
//   brokers: ["localhost:9092"],
// });

// const producer = kafka.producer();

// export default function () {
//     producer.produce({
//         topic: "input-topic",
//         messages: [
//             {
//                 key: "test-key",
//                 value: `k6 test msg ${Date.now()}`
//             }
//         ],
//     });
// }

// export function teardown() {
//     producer.close();
// }