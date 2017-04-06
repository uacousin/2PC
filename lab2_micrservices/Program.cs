using System;
using Npgsql;
class Program
{
    static void Main(string[] args)
    {
        string conf1 = "Server=localhost; User Id=postgres; Password=pwd; Database=DB1;";
        string conf2 = "Server=localhost; User Id=postgres; Password=pwd; Database=DB2;";
        string conf3 = "Server=localhost; User Id=postgres; Password=pwd; Database=DB3;";
        MyTM mytm = new MyTM(conf1, conf2, conf3);
        bool status1 = false, status2 = false, status3=false;
        try
        {
            
            status1 = mytm.PrepareTR1();
            status2 = mytm.PrepareTR2();
            status3 = mytm.PrepareTR3();
            mytm.CommitPreparedALL();


        }
        catch (NpgsqlException ex)
        {

            
            Console.WriteLine(ex.Message);
            if (status1)
                mytm.RollBackPrepared(mytm.cmd1, "my_tr");
            if (status2)
                mytm.RollBackPrepared(mytm.cmd2, "my_tr2");
            if (status3)
                mytm.RollBackPrepared(mytm.cmd2, "my_tr3");
        }
        finally
        {
            mytm.CloseConnections();
            Console.WriteLine("Connections closed");
        }


        Console.WriteLine("OK");

        Console.ReadKey();
    }
    class MyTM
    {
        private NpgsqlConnection DB1;
        private NpgsqlConnection DB2;
        private NpgsqlConnection DB3;
        public NpgsqlCommand cmd1;
        public NpgsqlCommand cmd2;
        public NpgsqlCommand cmd3;

        public MyTM(string conf1, string conf2, string conf3)

        {
            DB1 = new NpgsqlConnection(conf1);
            DB2 = new NpgsqlConnection(conf2);
            DB3 = new NpgsqlConnection(conf3);
            DB1.Open();
            DB2.Open();
            DB3.Open();
            cmd1 = DB1.CreateCommand();
            cmd2 = DB2.CreateCommand();
            cmd3 = DB3.CreateCommand();
        }
        public bool PrepareTR1()
        {
           //var tr1 = DB1.BeginTransaction();

            cmd1.CommandText = "BEGIN; INSERT INTO myschema.fly_booking ( client_name, fly_number, vfrom, vto, vdate) VALUES ('Oleh Fedyanovych', 'KLM 1382', 'KBP', 'AMS','" + DateTime.Now.ToString("yyyy'-'MM'-'dd'-'") + "' ); PREPARE TRANSACTION 'my_tr';";
            cmd1.ExecuteNonQuery();
            
            return true;

        }
        public bool PrepareTR3()
        {
            //var tr1 = DB1.BeginTransaction();

            cmd3.CommandText = "BEGIN; UPDATE myschema.account SET ammount=ammount-3; PREPARE TRANSACTION 'my_tr3';";
            cmd3.ExecuteNonQuery();

            return true;

        }
        public bool PrepareTR2()
        {
            //tr2 = DB2.BeginTransaction();

            cmd2.CommandText = "BEGIN; INSERT INTO myschema.hotel_booking (client_name, hotel_name, arrival, departure) VALUES ('Oleh Fedyanovych', 'Kiev', '" + DateTime.Now.ToString("yyyy'-'MM'-'dd'-'") + "','" + DateTime.Now.ToString("yyyy'-'MM'-'dd'-'") + " '); PREPARE TRANSACTION 'my_tr2';";
            cmd2.ExecuteNonQuery();
            
            return true;

        }
        public bool Voting()

        {
            return true;

        }
        
        public void CommitPreparedALL()
        {
                      

            cmd1.CommandText = "COMMIT PREPARED 'my_tr'";
            cmd1.ExecuteNonQuery();

            cmd2.CommandText = "COMMIT PREPARED 'my_tr2'";
            cmd2.ExecuteNonQuery();

            cmd3.CommandText = "COMMIT PREPARED 'my_tr3'";
            cmd3.ExecuteNonQuery();
        }

        public void RollBackPreparedALL()
        {
           

            cmd1.CommandText = "ROLLBACK PREPARED 'my_tr';";
            cmd1.ExecuteNonQuery();

            cmd2.CommandText = "ROLLBACK PREPARED 'my_tr2';";
            cmd2.ExecuteNonQuery();

            cmd3.CommandText = "ROLLBACK PREPARED 'my_tr3';";
            cmd3.ExecuteNonQuery();
        }
        public void RollBackPrepared(NpgsqlCommand cmd, string tr_name)
        {


            cmd.CommandText = "ROLLBACK PREPARED '"+ tr_name + "';";
            cmd.ExecuteNonQuery();

            
        }
        public void CommitPrepared(NpgsqlCommand cmd, string tr_name)
        {


            cmd.CommandText = "COMMIT PREPARED '" + tr_name + "';";
            cmd.ExecuteNonQuery();
        }
        public void CloseConnections()
        {
            DB1.Close();
            DB2.Close();
        }

    }
}