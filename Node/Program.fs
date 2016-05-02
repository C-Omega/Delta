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
    let a = RemoteProcessor(C_Omega.Comm.bbsr sra)
    let ct = new System.Threading.CancellationTokenSource()
    let b = Async.Start(RemoteProcessorHandler(srb),ct.Token)
    let p = 
        a.start(
            <@ {
                on_create = (function 
                    |{pid = p;parent = 0uL} -> (%caller) (Calls.fork p) |> ignore; ((%caller) (Calls.get_sr 0uL)|>unbox<bsr>).send [|127uy|] |> ignore
                    |_ -> spin 10;((%caller) (Calls.get_sr 0uL)|>unbox<bsr>).send [|69uy|] |>ignore)
                channel = ignore
                name="lol"
                get_state = (fun () -> Sig.START)
            } @>)
        |> a.get_pid
        |> Option.get
    (a.call(Calls.get_sr(0uL)):?>Comm.bsr).receive()|>printfn "%A"
    (a.call(Calls.get_sr(0uL)):?>Comm.bsr).receive()|>printfn "%A"
    p.get_state()|>printfn "%A"
    p.channel.Send Sig.KILL
    spin 10
    p.get_state()|>printfn "%A"
    0 // return an integer exit code