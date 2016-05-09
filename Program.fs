open C_Omega
open C_Omega.Delta
open C_Omega.Delta.Processes
open C_Omega.Net
open C_Omega.Helpers
open C_Omega.Comm
let cts = 
    Array.concat[|
        [|
            AllowType typeof<ProcessCreationOptions>
            AllowType typeof<string>
            AllowType typeof<char>
            AllowType typeof<byte>
            AllowType typeof<sbyte>
            AllowType typeof<uint16>
            AllowType typeof<int16>
            AllowType typeof<uint32>
            AllowType typeof<int32>
            AllowType typeof<uint64>
            AllowType typeof<int64>
            AllowType typeof<bool>
            AllowType typeof<System.Void>
            AllowType typeof<unit>
            AllowModule "C_Omega.Helpers"
        |]
        [|
            DenyNamespace "*"
        |]
        [|

        |]
    |]

[<EntryPoint>]
let main argv = 
    let epa = ep loopbackv4 9001
    let epb = ep loopbackv4 9002
    let sra = (udpIP epa epb)//.Log Logger.Printn
    let srb = (udpIP epb epa)//.Log Logger.Printn
    let a = RemoteProcessor(new bbsr(sra))
    let ct = new System.Threading.CancellationTokenSource()
    let b = Async.Start(RemoteProcessorHandler(srb),ct.Token)
    let rec factorial = function |0|1 -> 1|x -> x*factorial(x-1)
    let procp (caller:Calls.ProcessorCall -> obj) (p:Process) =
      [|
       for i = 1 to 10 do
       yield caller (Calls.fork p.pid) |> unbox<uint64>
      |]
      |>Array.Parallel.mapi(fun n (i:uint64) ->
                         let sr = caller(Calls.proc_sr(i))|>unbox<bsr>
                         sr.send(_int32_b n)
                         System.BitConverter.ToDouble(sr.receive(),0)
                            )
      |>Array.sum
      |>System.BitConverter.GetBytes
      |>((caller)(Calls.proc_sr p.pid) |> unbox<bsr>).send
    let procc (caller:Calls.ProcessorCall -> obj) (p:Process) =
      let sr = Calls.proc_sr(p.pid)|>unbox<bsr>
      let i = sr.receive() |> _b_int32
      float(i)/float(factorial i)
      |> System.BitConverter.GetBytes
      |> sr.send
    let p =   
        a.start(
            <@
            let procp (caller:Calls.ProcessorCall -> obj) (p:Process) =
                [|
                 for i = 1 to 10 do
                 yield caller (Calls.fork p.pid) |> unbox<uint64>
                |]
                |>Array.Parallel.mapi(fun n (i:uint64) ->
                                   let sr = caller(Calls.proc_sr(i))|>unbox<bsr>
                                   sr.send(_int32_b n)
                                   System.BitConverter.ToDouble(sr.receive(),0)
                                      )
                |>Array.sum
                |>System.BitConverter.GetBytes
                |>((caller)(Calls.proc_sr p.pid) |> unbox<bsr>).send
            let procc (caller:Calls.ProcessorCall -> obj) (p:Process) =
                let sr = caller(Calls.proc_sr(p.pid))|>unbox<bsr>
                let i = sr.receive() |> _b_int32
                float(i)/float(factorial i)
                |> System.BitConverter.GetBytes
                |> sr.send
            {
                    on_create = prochelp(procp(%caller), procc(%caller))
                    channel = ignore
                    name="lol"
                    get_state = (fun () -> Sig.START)

            } @>)
        
    (a.call(Calls.proc_sr(p)):?>Comm.bsr).receive()|>printfn "%A"
    
    0 // return an integer exit code
