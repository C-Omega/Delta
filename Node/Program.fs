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
    let sra = (udpIP epa epb).Log Logger.Printn
    let srb = (udpIP epb epa).Log Logger.Printn
    let a = RemoteProcessor(C_Omega.Comm.bbsr sra)
    let ct = new System.Threading.CancellationTokenSource()
    let b = Async.Start(RemoteProcessorHandler(srb),ct.Token)
    let p = a.start(<@ {on_create = (fun _ -> while true do spin 1000); channel = ignore; name="lol";get_state = (fun () -> Sig.START)}  @>,sra)
    spin 3000
    p.channel.Send Sig.KILL
    spin 1000
    p.get_state()|>printfn "%A"
    exit 0 
    0 // return an integer exit code

