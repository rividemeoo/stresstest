using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using MySqlConnector;

class Program
{
    //static string connectionString = "datasource=usdc2-lab-drproxy01.ring2.com;port=3306;username=ctnr_user;password=Ct4JkD@ta;database=number_routing_engr";
    static string connectionString = "server=10.62.1.77;user=root;database=ct_number_routing2;port=3306;password=7c225-201";

    static List<RoutingData> routingDataList = new List<RoutingData>();

    public class RoutingData
    {
        public int ThreadId { get; set; }
        public int Iteration { get; set; }
        public string Ddi { get; set; }
        public string RouteType { get; set; }
        public string Destination { get; set; }
        public int Priority { get; set; }
        public double ElapsedTimeMilliseconds { get; set; }
    }

    static void Main(string[] args)
    {
        int numThreads = 50; // Change this to the desired number of threads
        List<Thread> threads = new List<Thread>();

        for (int i = 0; i < numThreads; i++)
        {
            Thread thread = new Thread(new ThreadStart(StressTest));
            threads.Add(thread);
            thread.Start();
        }

        foreach (Thread thread in threads)
        {
            thread.Join();
        }

        WriteListToCsv(routingDataList, "D:/temp/output.csv");
        Console.WriteLine("All threads completed.");
    }

    public static void StressTest()
    {
        Random random = new Random();
        MySqlConnection connection = new MySqlConnection(connectionString);
        connection.Open();
        int iteration = 1000;

        for (int i = 0; i < iteration; i++)
        {
            try
            {
                // Select a random ddi from the 'numbers' table
                MySqlCommand selectDdiCommand = connection.CreateCommand();
                selectDdiCommand.CommandText = "SELECT ddi FROM Numbers where siptrunkid is not null ORDER BY RAND() LIMIT 1";
                object randomDdi = selectDdiCommand.ExecuteScalar();

                if (randomDdi != null)
                {
                    // Use the randomly selected ddi to query the 'RoutingProxyReadModel' table
                    MySqlCommand selectFromRoutingProxyCommand = connection.CreateCommand();
                    selectFromRoutingProxyCommand.CommandText = "SELECT * FROM RoutingProxyReadModel WHERE Ddi = @ddi";
                    selectFromRoutingProxyCommand.Parameters.AddWithValue("@ddi", randomDdi);

                    DateTime startTime = DateTime.Now;
                    MySqlDataReader reader = selectFromRoutingProxyCommand.ExecuteReader();
                    TimeSpan elapsedTime = DateTime.Now - startTime;

                    while (reader.Read())
                    {
                        Console.WriteLine("Thread {0}: Iteration: {1} - Ddi: {2}, Success, Elapsed Time: {6} milliseconds",
                            Thread.CurrentThread.ManagedThreadId, i + 1, reader["Ddi"], reader["RouteType"], reader["Destination"], reader["Priority"], elapsedTime.TotalMilliseconds);
                        //elapsedTime = DateTime.Now - startTime;
                        RoutingData data = new RoutingData
                        {
                            ThreadId = Thread.CurrentThread.ManagedThreadId,
                            Iteration = i + 1,
                            Ddi = reader["Ddi"].ToString(),
                            //RouteType = reader["RouteType"].ToString(),
                            RouteType = "Success",
                            Destination = reader["Destination"].ToString(),
                            Priority = Convert.ToInt32(reader["Priority"]),
                            ElapsedTimeMilliseconds = elapsedTime.TotalMilliseconds
                        };
                        lock (routingDataList)
                        {
                            routingDataList.Add(data);
                        }
                        break;
                    }

                    reader.Close();
                }
                else
                {
                    Console.WriteLine("Thread {0}: No ddi found in the 'numbers' table.", Thread.CurrentThread.ManagedThreadId);
                }
            }
            catch (Exception ex)
            {
                RoutingData data = new RoutingData
                {
                    ThreadId = Thread.CurrentThread.ManagedThreadId,
                    Iteration = i + 1,
                    Ddi = "",
                    RouteType = ex.Message,
                    Destination = "",
                    Priority = 0,
                    ElapsedTimeMilliseconds = 0
                };
                lock (routingDataList)
                {
                    routingDataList.Add(data);
                }
                Console.WriteLine("Thread {0}: Exception occurred: {1}", Thread.CurrentThread.ManagedThreadId, ex.Message);
            }
        }
        connection.Close();
    }


    static void WriteListToCsv(List<RoutingData> list, string filePath)
    {
        using (StreamWriter writer = new StreamWriter(filePath))
        {
            StringBuilder header = new StringBuilder();
            header.Append("ThreadId,Iteration,Ddi,Status,ElapsedTimeMilliseconds");
            writer.WriteLine(header.ToString());

            foreach (var data in list)
            {
                StringBuilder line = new StringBuilder();
                line.Append($"{data.ThreadId},{data.Iteration},{data.Ddi},{data.RouteType},{data.ElapsedTimeMilliseconds}");
                writer.WriteLine(line.ToString());
            }
        }
    }
}
