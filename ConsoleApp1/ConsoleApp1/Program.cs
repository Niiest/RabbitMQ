using RabbitMQ.Stream.Client.Reliable;
using RabbitMQ.Stream.Client;
using System.Text;
using System.Buffers;
using System.Net;
using RabbitMQ.Stream.Client.AMQP;
using Microsoft.VisualBasic;

namespace ConsoleApp1
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Start().GetAwaiter().GetResult();
        }

        public static async Task Start()
        {
            var config = new StreamSystemConfig
            {
                UserName = "guest",
                Password = "guest",
                VirtualHost = "/",
                Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) }
            };
            // Connect to the broker and create the system object
            // the entry point for the client.
            // Create it once and reuse it.
            var system = await StreamSystem.Create(config);

            const string stream = "my_first_stream";

            // Create the stream. It is important to put some retention policy 
            // in this case is 200000 bytes.
            await system.CreateStream(new StreamSpec(stream)
            {
                MaxLengthBytes = 200000,
            });

            var producer = await Producer.Create(
                new ProducerConfig(system, stream)
                {
                    Reference = Guid.NewGuid().ToString(),


                    // Receive the confirmation of the messages sent
                    ConfirmationHandler = confirmation =>
                    {
                        switch (confirmation.Status)
                        {
                            // ConfirmationStatus.Confirmed: The message was successfully sent
                            case ConfirmationStatus.Confirmed:
                                Console.WriteLine($"Message {confirmation.PublishingId} confirmed");
                                break;
                            // There is an error during the sending of the message
                            case ConfirmationStatus.WaitForConfirmation:
                            case ConfirmationStatus.ClientTimeoutError
                                : // The client didn't receive the confirmation in time. 
                                  // but it doesn't mean that the message was not sent
                                  // maybe the broker needs more time to confirm the message
                                  // see TimeoutMessageAfter in the ProducerConfig
                            case ConfirmationStatus.StreamNotAvailable:
                            case ConfirmationStatus.InternalError:
                            case ConfirmationStatus.AccessRefused:
                            case ConfirmationStatus.PreconditionFailed:
                            case ConfirmationStatus.PublisherDoesNotExist:
                            case ConfirmationStatus.UndefinedError:
                            default:
                                Console.WriteLine(
                                    $"Message  {confirmation.PublishingId} not confirmed. Error {confirmation.Status}");
                                break;
                        }

                        return Task.CompletedTask;
                    }
                });

            
            // Publish the messages
            for (var i = 0; i < 100; i++)
            {
                ReadOnlySequence<byte> span = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"hello {i}"));
                var message = new Message(new Data(span));
                await producer.Send(message);
            }

            // not mandatory. Just to show the confirmation
            Thread.Sleep(TimeSpan.FromSeconds(1));

            // Create a consumer
            var consumer = await Consumer.Create(
                new ConsumerConfig(system, stream)
                {
                    Reference = "my_consumer",
                    // Consume the stream from the beginning 
                    // See also other OffsetSpec 
                    OffsetSpec = new OffsetTypeOffset(15),
                    // Receive the messages
                    MessageHandler = async (sourceStream, consumer, ctx, message) =>
                    {
                        if (ctx.Offset % 2 == 0)
                            return;

                        Console.WriteLine(
                            $"message: coming from {sourceStream} data: {Encoding.Default.GetString(message.Data.Contents.ToArray())} - consumed");

                        await Task.CompletedTask;
                    }
                });

            // Create a consumer
            var consumer2 = await Consumer.Create(
                new ConsumerConfig(system, stream)
                {
                    Reference = "my_consumer",
                    // Consume the stream from the beginning 
                    // See also other OffsetSpec 
                    OffsetSpec = new OffsetTypeOffset(16),
                    // Receive the messages
                    MessageHandler = async (sourceStream, consumer, ctx, message) =>
                    {
                        if (ctx.Offset % 2 == 1)
                            return;

                        Console.WriteLine(
                            $"message: coming from {sourceStream} data: {Encoding.Default.GetString(message.Data.Contents.ToArray())} - consumed");

                        await Task.CompletedTask;
                    }
                });

            Console.WriteLine($"Press to stop");
            Console.ReadLine();

            await producer.Close();
            await consumer.Close();
            await consumer2.Close();
            await system.DeleteStream(stream);
            await system.Close();
        }
    }
}