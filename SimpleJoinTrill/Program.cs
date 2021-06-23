using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Runtime.InteropServices;
using System.Xml.Xsl;
using Microsoft.StreamProcessing;


namespace SimpleJoinTrill
{
    class Program
    {
        class MyData
        {
            public long Time;
            public int Payload;

            public MyData(long time, int payload)
            {
                this.Time = time;
                this.Payload = payload;
            }
        }

        class MyObservable1 : IObservable<MyData>
        {
            public IDisposable Subscribe(IObserver<MyData> observer)
            {
                using (var reader = new StreamReader(@"/root/SimpleJoinTrill/random_stream1/750_thousand_random.csv"))
                //using (var reader = new StreamReader(@"C:\Users\yudis\Documents\university\Summer2021\Code\pysparkPrograms\data\random_stream1\250_thousand_random.csv"))
                
                {
                  reader.ReadLine();
                  while (!reader.EndOfStream)
                  {
                      var line = reader.ReadLine();
                      //Console.WriteLine(line);
                      var values = line.Split(',');
                      var data = new MyData(long.Parse(values[0]), int.Parse(values[1]));
                      observer.OnNext(data);

                  }
                }
                observer.OnCompleted();
                return Disposable.Empty;
            }
        }
        
        class MyObservable2 : IObservable<MyData>
        {
            public IDisposable Subscribe(IObserver<MyData> observer)
            {
                using (var reader = new StreamReader(@"/root/SimpleJoinTrill/random_stream2/750_thousand_random2.csv"))
                //using (var reader = new StreamReader(@"C:\Users\yudis\Documents\university\Summer2021\Code\pysparkPrograms\data\random_stream2\250_thousand_random2.csv"))
                
                {
                    reader.ReadLine();
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        //Console.WriteLine(line);
                        var values = line.Split(',');
                        var data = new MyData(long.Parse(values[0]), int.Parse(values[1]));
                        observer.OnNext(data);

                    }
                }
                observer.OnCompleted();
                return Disposable.Empty;
            }
        } 
        
        static void Main(string[] args)
        {

            var randomObservable1 = new MyObservable1();
            var randomStreamable1 =
                randomObservable1.ToTemporalStreamable(e => e.Time, e => e.Time + 1)
                    .Cache();
            
            var randomObservable2 = new MyObservable2();
            var randomStreamable2 =
                randomObservable2.ToTemporalStreamable(e => e.Time, e => e.Time + 1)
                    .Cache();
            

            var sw = new Stopwatch();
            sw.Start();
            
            //int WindowSize = 10;
            var result = randomStreamable1.Join(randomStreamable2,
                (left, right) => new {left, right}).Where(e => e.left.Payload > e.right.Payload);

            //result.ToStreamEventObservable()
            //    .ForEachAsync(e => Console.WriteLine(e.ToString()));

            result
                .ToStreamEventObservable()
                .Wait();

            sw.Stop();
            Console.WriteLine(sw.Elapsed.TotalSeconds);
        }
    }
    


}