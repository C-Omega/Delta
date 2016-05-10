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
let main(argc) =
    let epa = ep loopbackv4 9001
    let epb = ep loopbackv4 9002
    let sra = (udpIP epa epb)//.Log Logger.Printn
    let srb = (udpIP epb epa)//.Log Logger.Printn
    let a = RemoteProcessor(new bbsr(sra))
    let ct = new System.Threading.CancellationTokenSource()
    let b = Async.Start(RemoteProcessorHandler(srb),ct.Token)
    let rec factorial = function |0|1 -> 1|x -> x*factorial(x-1)
    let p =   
        a.start(
            <@
            let procp  (p:Process) =
                [|
                 for i = 1 to 1 do
                 yield (%caller) (Calls.fork p.pid) |> unbox<uint64>
                |]
                |> Array.map(fun (n:uint64) ->
                                    let sr = (%caller)(Calls.proc_sr(p.pid,n))|>unbox<bsr>
                                    printfn "Got sr"
                                    sr.send(_int32_b 1)
                                    let r = sr.receive()
                                    if isNull(r) then 
                                        failwith "NOPE!"
                                    printfn "GOTVAL"
                                    System.Console.Write(r)
                                    let v = System.BitConverter.ToDouble(r,0)//SArray.chunkBySize 4 r|> Array.map _b_int32)
                                    printfn "DONE!%A" v
                                    v
                                    )
                |>Array.sum
                |>printfn "%A"
                //|>System.BitConverter.GetBytes
                //|>((%caller)(Calls.get_sr p.pid) |> unbox<bsr>).send
            let procc (p:Process) =
                printfn "Started child %A" p.pid
                let sr = (%caller)(Calls.proc_sr(p.pid,p.parent))|>unbox<bsr>
                let i = sr.receive() |> _b_int32
                printfn "Recieved"
                System.BitConverter.GetBytes(float i/float (factorial i))
                |> sr.send
            {
                    on_create = function |IsParent as p -> procp p |p -> procc p//prochelp(procp, procc)
                    channel = ignore
                    name="lol"
                    get_state = (fun () -> Sig.START)

            } @>)
    let v = (a.call(Calls.get_sr(p)):?>Comm.bsr)
    v.receive()|>printfn "%A"
    0 // return an integer exit code
