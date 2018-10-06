using Apache.NMS;
using Apache.NMS.Util;
using System;
using System.Configuration;
using System.Threading;

namespace ActiveMQAsyncListener
{
    internal class Program
    {
        protected static AutoResetEvent semaphore = new AutoResetEvent(false);
        protected static ITextMessage message;
        protected static TimeSpan receiveTimeout = TimeSpan.FromSeconds(int.MaxValue);
        public static string TerminalId = "";
        public static int Counter;
        public static string connectionStr = "";
        public static string activeMQUri = "";
        public static string queueName = "";
        public static string Terminal = "";

        private static void Main()
        {
            try
            {
                Console.BackgroundColor = ConsoleColor.DarkBlue;
                Console.ForegroundColor = ConsoleColor.White;
                TerminalId = ConfigurationManager.AppSettings[nameof(TerminalId)];
                activeMQUri = ConfigurationManager.AppSettings["ActiveMQUri"];
                queueName = ConfigurationManager.AppSettings["QueueName"];
                Terminal = ConfigurationManager.AppSettings["TerminalName"];
                connectionStr = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
                Console.WriteLine("|--------------------------------------------------------------------|");
                Console.Write("|----------------------");
                Console.ResetColor();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.BackgroundColor = ConsoleColor.White;
                Console.Write(" Listener Application ");
                Console.ResetColor();
                Console.BackgroundColor = ConsoleColor.DarkBlue;
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine("------------------------|");
                Console.WriteLine("|----------------------- Active MQ Listener -------------------------|");
                Console.WriteLine("|--------------------------------------------------------------------|");
                Console.WriteLine("|> " + spaceChecker("Terminal: " + Terminal + " ") + "|");
                Console.WriteLine("|--------------------------------------------------------------------|");
                Console.WriteLine("|> " + spaceChecker("Active MQ : " + activeMQUri + " ") + "|");
                Console.WriteLine("|--------------------------------------------------------------------|");
                Console.WriteLine("|> " + spaceChecker("Queue Name : " + queueName + " ") + "|");
                Console.WriteLine("|--------------------------------------------------------------------|");
                Console.WriteLine("|> " + spaceChecker("Terminal Id : " + TerminalId + " ") + "|");
                Console.WriteLine("|--------------------------------------------------------------------|");
                Console.WriteLine("|--------------------------------------------------------------------|");
                Console.WriteLine("|> " + spaceChecker("Application started at : " + DateTime.Now.ToString() + " ") + "|");
                Console.WriteLine("|--------------------------------------------------------------------|");
                Console.Write("|-------------------------");
                Console.ResetColor();
                Console.ForegroundColor = ConsoleColor.White;
                Console.BackgroundColor = ConsoleColor.DarkGreen;
                Console.Write(" STATUS : RUNNING ");
                Console.ResetColor();
                Console.BackgroundColor = ConsoleColor.DarkBlue;
                Console.ForegroundColor = ConsoleColor.White;
                Console.WriteLine("-------------------------|");
                Console.WriteLine("|--------------------------------------------------------------------|");

                ReaderMetod();
                Console.ReadKey();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:   " + ex.ToString());
                Console.ReadKey();
            }
        }

        public static string spaceChecker(string word)
        {
            if (word.Length < 70)
            {
                var a = 66 - word.Length;
                for (int i = 0; i < a; i++)
                {
                    word += "-";
                }
            }
            return word;
        }

        public static void ReaderMetod()
        {
            try
            {
                var connecturi = new Uri(activeMQUri);
                IConnectionFactory factory = new NMSConnectionFactory(connecturi);

                using (IConnection connection = factory.CreateConnection())
                using (ISession session = connection.CreateSession())
                {
                    var destination = SessionUtil.GetDestination(session, queueName);

                    using (IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        connection.Start();
                        var OldMessage = "";
                        waitNewOne:
                        consumer.Listener += new MessageListener(OnMessage);
                        semaphore.WaitOne(int.MaxValue, true);
                        if (message == null)
                        {
                            Console.WriteLine("No message received!");
                        }
                        else
                        {
                            if (OldMessage != message.NMSMessageId)
                            {
                                //message.Text mesaj icerigini bu şekilde yakalayıp işlem yapabilirsiniz.

                                Counter++;
                                Console.Write("\r|> " + spaceChecker("Total Message |---> " + Counter.ToString() + " <---| Last at : " + DateTime.Now.ToString()) + "|");
                                OldMessage = message.NMSMessageId;
                            }
                            goto waitNewOne;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:   " + ex.ToString());
                Console.ReadKey();
            }
        }

        protected static void OnMessage(IMessage receivedMsg)
        {
            message = receivedMsg as ITextMessage;
            semaphore.Set();
        }
    }
}
