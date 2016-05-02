namespace C_Omega.Delta
open C_Omega
open C_Omega.Helpers
open C_Omega.ArraySliceImprovement
module Extensions = 
    module Option = 
        let ofBoolObjTuple (a,b) = if a then Some(b) else None
module Calls = 
    ///A record type that represents a processor call
    type ProcessorCall = {call : uint32; arg : obj}
    let get_sr (id:uint64) = {call = 0u; arg = box id}
    let fork pid = {call = 1u; arg = box pid}
module Processes = 
    open Microsoft.FSharp.Quotations
    open Extensions
    open Calls
    ///An enumeration of the signals that can be sent
    type Sig = 
        |START = 0uy
        |RESTART = 1uy
        |STOP = 2uy
        |PAUSE = 3uy
        |RESUME = 4uy
        |END = 5uy
        |KILL = 6uy
        |ERR = 7uy
        |BABY = 8uy
        |NONEXIST = 255uy
    type CheckType = 
        |AllowNamespace of string 
        |DenyNamespace of string 
        |AllowModule of string
        |AllowType of System.Type 
        |DenyType of System.Type
        |AllowDeriv of System.Type
        |DenyDeriv of System.Type
        static member Check v (cts : seq<CheckType>) = 
            ///#credit <http://www.codeproject.com/Articles/11556/Converting-Wildcards-to-Regexes>
            let r (b,a) = System.Text.RegularExpressions.Regex("^"+System.Text.RegularExpressions.Regex.Escape(a).Replace("\\*",".*").Replace("\\?", ".")+"$").IsMatch(b)
            function 
                |AllowType(t:System.Type) when v = t -> Some true
                |DenyType(t:System.Type) when v = t -> Some false
                |AllowNamespace(m:string) when r(v.Namespace, m) -> Some true
                |DenyNamespace(m:string) when r(v.Namespace, m) -> Some false
                |AllowModule(m:string) when r(v.FullName, m) -> Some true
                |AllowDeriv(t:System.Type) when v.IsSubclassOf(t.BaseType) -> Some true
                |DenyDeriv(t:System.Type) when v.IsSubclassOf(t.BaseType) -> Some false
                |_ -> None
            |> Seq.tryPick
            <| cts
            |> function |Some(v) -> v |_ -> false
    ///A channel for sending signals
    type SigChannel(signalHandler : Sig -> unit) = member x.Send (s:Sig) = async{signalHandler s}|>Async.Start
    let callerVar = Var("caller",typeof<ProcessorCall -> obj>)
    let caller : Expr<ProcessorCall -> obj> = Expr.Var callerVar |> Expr.Cast
    ///A record type cotaining the pid, channel and state of a process
    type Process = 
        {
            pid : uint64
            channel : SigChannel
            get_state : unit -> Sig
            name : string
            pco : Expr<ProcessCreationOptions>
            parent : uint64
        }
    ///Options required to create a process
    and ProcessCreationOptions = 
        {
            channel : Sig -> unit
            name : string
            get_state : unit -> Sig
            on_create : Process -> unit
        }
        static member Mash(v:Expr<ProcessCreationOptions>) = 
            using(new System.IO.MemoryStream()) (fun s ->
                System.Runtime.Serialization.Formatters.Binary.BinaryFormatter().Serialize(s,v)
                s.ToArray())
        static member Express(v,caller) = 
            let rec outer a (d:System.Collections.Generic.Dictionary<Var,obj>)= 
                let rec inner = function
                    |Patterns.Value(v,t) -> v
                    |Patterns.Var(s) -> Seq.pick(fun (v:System.Collections.Generic.KeyValuePair<Var,obj>) -> if v.Key.Name = s.Name then Some v.Value else None) d
                    |Patterns.VarSet(v,e) -> d.[v] <- (inner e);box()
                    |Patterns.Sequential(a,b) -> inner a |>ignore;inner b
                    |Patterns.PropertyGet(Some(v),a,b) -> a.GetValue(inner v,b|>Seq.map inner|>Seq.toArray)
                    |Patterns.PropertyGet(None,a,b) -> a.GetValue(null,b|>Seq.map inner|>Seq.toArray)
                    |Patterns.PropertySet(Some(v),a,b,c) -> a.SetValue(inner v,inner c,b|>Seq.map inner|>Seq.toArray)|>box
                    |Patterns.PropertySet(None,a,b,c) -> a.SetValue(null,inner c,b|>Seq.map inner|>Seq.toArray)|>box
                    |Patterns.Call(v,a,b) -> a.Invoke(v,b|>Seq.map inner|>Seq.toArray)
                    |Patterns.NewRecord(t,b) -> Reflection.FSharpValue.MakeRecord(t,b|>Seq.map (inner >> box)|>Seq.toArray)
                    |Patterns.Let(v,a,b) -> d.Add(v,inner a); inner b
                    |Patterns.Coerce(e,t) -> (inner e)
                    |Patterns.NewObject(t,b) -> b|>Seq.map (inner >> box)|>Seq.toArray|>t.Invoke
                    |Patterns.Lambda(a,l) -> 
                        Reflection.FSharpValue.MakeFunction(Reflection.FSharpType.MakeFunctionType(a.Type,l.Type),(fun o -> 
                            let d' = System.Collections.Generic.Dictionary(d)
                            d'.Add(a,o)
                            outer(l) d'))
                    |Patterns.Application(f,a) -> let f' = (inner f) in f'.GetType().GetMethod("Invoke",[|a.Type|]).Invoke(f',[|inner a|])
                    |Patterns.DefaultValue(t) -> System.Activator.CreateInstance(t)
                    |Patterns.WhileLoop(v,s) -> (while inner v |> unbox do inner s|>ignore)|>box
                    |Patterns.IfThenElse(e,a,b) -> if inner e |> unbox then inner a else inner b
                    //#credit https://github.com/SwensenSoftware/unquote
                    |Patterns.NewArray(t,e) as e' -> 
                        let arr = System.Array.CreateInstance(t,e.Length)
                        let f (a,b) = e'.Type.GetMethod("Set").Invoke(a,b)
                        List.map inner e |> Seq.iteri(fun i v -> f(arr, [|box i;box v|]) |> ignore)
                        box arr
                    |e -> failwithf "%A" e
                inner a
            outer v (new System.Collections.Generic.Dictionary<Var,obj>(dict [callerVar,box caller])) |> unbox<ProcessCreationOptions>
        static member Express(v,ct,caller) = 
            let f a = if not(isNull(a)) && not(CheckType.Check a ct) then failwithf "BAD: %A" a
            let rec outer a (d:System.Collections.Generic.Dictionary<Var,obj>)= 
                let rec inner = function
                    |Patterns.Value(v,t) -> 
                        f t
                        f t.DeclaringType
                        v
                    |Patterns.Var(v) -> d.[v]
                    |Patterns.VarSet(v,e) -> d.[v] <- (inner e);box()
                    |Patterns.Sequential(a,b) -> inner a |>ignore;inner b
                    |Patterns.PropertyGet(Some(v),a,b) -> 
                        f a.PropertyType
                        f a.DeclaringType
                        a.GetValue(inner v,b|>Seq.map inner|>Seq.toArray)
                    |Patterns.PropertyGet(None,a,b) -> 
                        f a.PropertyType
                        a.GetValue(null,b|>Seq.map inner|>Seq.toArray)
                    |Patterns.PropertySet(Some(v),a,b,c) -> 
                        f a.PropertyType
                        f a.DeclaringType
                        a.SetValue(inner v,inner c,b|>Seq.map inner|>Seq.toArray)|>box
                    |Patterns.PropertySet(None,a,b,c) -> 
                        f a.PropertyType
                        f a.DeclaringType
                        a.SetValue(null,inner c,b|>Seq.map inner|>Seq.toArray)|>box
                    |Patterns.Call(v,a,b) -> 
                        f a.ReturnType
                        f a.DeclaringType
                        a.Invoke(v,b|>Seq.map inner|>Seq.toArray)
                    |Patterns.NewRecord(t,b) ->
                        f t
                        Reflection.FSharpValue.MakeRecord(t,b|>Seq.map (inner >> box)|>Seq.toArray)
                    |Patterns.Let(v,a,b) -> d.Add(v,inner a); inner b
                    |Patterns.Coerce(e,t) -> f t; (inner e)
                    |Patterns.NewObject(t,b) -> f t.DeclaringType; b|>Seq.map (inner >> box)|>Seq.toArray|>t.Invoke
                    |Patterns.Lambda(a,l) -> 
                        Reflection.FSharpValue.MakeFunction(Reflection.FSharpType.MakeFunctionType(a.Type,l.Type),(fun o -> 
                            let d' = System.Collections.Generic.Dictionary(d)
                            d'.Add(a,o)
                            outer(l) d'))
                    |Patterns.Application(f,a) -> let f' = (inner f) in f'.GetType().GetMethod("Invoke",[|a.Type|]).Invoke(f',[|inner a|])
                    |Patterns.DefaultValue(t) -> f t;System.Activator.CreateInstance(t)
                    |Patterns.WhileLoop(v,s) -> (while inner v |> unbox do inner s|>ignore)|>box
                    |Patterns.IfThenElse(v,a,b) -> if inner v |> unbox then inner a else inner b
                    |e -> failwithf "%A" e
                inner a
            outer v (new System.Collections.Generic.Dictionary<Var,obj>(dict [callerVar,box caller])) |> unbox<ProcessCreationOptions>
        static member UnMash(b:byte[]) = using(new System.IO.MemoryStream(b)) (System.Runtime.Serialization.Formatters.Binary.BinaryFormatter().Deserialize>>unbox)

    ///An interface that represents a processor.
    type IProcessor = 
        abstract start : Expr<ProcessCreationOptions> -> uint64
        abstract get_all : unit -> uint64[]
        abstract get_pid : uint64 -> Process option
        abstract kill : uint64 -> unit
        abstract call : ProcessorCall -> obj
    exception BadCommunication
    type RemoteCommunication = 
        |Start of Expr<ProcessCreationOptions>
        |Started of uint64
        |SetChannel of uint64 * Sig
        |GetState of uint64
        |GotState of Sig
        |GetName of uint64
        |GotName of string
        |GetAll
        |GotAll of uint64[]
        |Msg of byte[]
        |GetPCO of uint64
        |GotPCO of Expr<ProcessCreationOptions>
        |GetParent of uint64
        |GotParent of uint64
        |No
        member x.GetBytes() = 
            match x with
            |Start(v) -> Array.append [|0uy|] (ProcessCreationOptions.Mash v)
            |Started(i) -> Array.append [|1uy|] (_uint64_b i)
            |SetChannel(i,s) -> Array.append [|2uy;byte s|] (_uint64_b i)
            |GetState(i) -> Array.append [|3uy|] (_uint64_b i)
            |GotState(s) -> [|4uy;byte s|]
            |GetName(i) -> Array.append [|5uy|] (_uint64_b i)
            |GotName(s) -> Array.append [|6uy|] (System.Text.ASCIIEncoding.UTF8.GetBytes s)
            |GetAll -> [|7uy|]
            |GotAll(b) -> b |> Array.map byte |> Array.append [|8uy|]
            |Msg(b) -> Array.append [|9uy|] b
            |GetPCO(i) -> Array.append [|10uy|] (_uint64_b i)
            |GotPCO(p) -> Array.append [|11uy|] (ProcessCreationOptions.Mash p)
            |GetParent(i) -> Array.append [|12uy|] (_uint64_b i)
            |GotParent(i) -> Array.append [|13uy|] (_uint64_b i)
            |No -> [|255uy|]
        static member OfBytes(b':byte[]) = 
            match b'.[0],if b'.Length > 1 then Some(b'.[1..]) else None with
            |0uy, Some(b) -> Start(ProcessCreationOptions.UnMash b)
            |1uy, Some(b) -> Started(_b_uint64 b)
            |2uy, Some(b) -> SetChannel(_b_uint64 b.[1..],enum b.[0])
            |3uy, Some(b) -> GetState(_b_uint64 b)
            |4uy, Some(b) -> GotState(Array.head b |> enum)
            |5uy, Some(b) -> GetName(_b_uint64 b)
            |6uy, Some(b) -> GotName(System.Text.ASCIIEncoding.UTF8.GetString(b))
            |7uy, None    -> GetAll
            |8uy, Some(b) -> b |>Array.chunkBySize 8 |> Array.map _b_uint64 |> GotAll
            |9uy, Some(b) -> Msg(b)
            |10uy,Some(b) -> GetPCO(_b_uint64 b)
            |11uy,Some(b) -> GotPCO(ProcessCreationOptions.UnMash b)
            |12uy,Some(b) -> GetParent(_b_uint64 b)
            |13uy,Some(b) -> GotParent(_b_uint64 b)
            |255uy, None -> No
            |_ -> raise BadCommunication
    let LocalProcessor(getsr : (uint64 -> Comm.bsr) option) =
        let getsr = defaultArg getsr (ignore >> Comm.loopback<byte[]>)
        let srs = new System.Collections.Generic.Dictionary<uint64,Comm.bsr>()
        let reg f (ct:System.Threading.CancellationToken) = 
            let v = System.Threading.Thread(System.Threading.ThreadStart(f))
            ct.Register(new System.Action(v.Abort))|>ignore
            v.Start()
        let processes = new System.Collections.Generic.Dictionary<uint64, Process>()
        let nextpid =
            let pid = ref 1uL
            let rec inner() = if processes.ContainsKey(!pid) then pid := !pid + 1uL;inner() else !pid
            fun() -> lock pid inner
        let cts = new System.Collections.Generic.Dictionary<uint64, (unit -> unit)>()
        {new IProcessor with
            member x.start(pco') = 
                let pid = nextpid() 
                let ct = new System.Threading.CancellationTokenSource()
                lock cts (fun () -> cts.Add(pid,ct.Cancel))
                (reg(fun() ->
                    let pco = ProcessCreationOptions.Express(pco',x.call)
                    let v = 
                        {
                            pid = pid
                            channel = SigChannel(function
                                    |Sig.KILL -> 
                                        ct.Cancel()
                                        processes.Remove(pid)|>ignore
                                        cts.Remove(pid)|>ignore
                                    |s -> reg(fun() -> pco.channel(s)) ct.Token)
                            get_state = pco.get_state
                            name = pco.name
                            pco = pco'
                            parent = 0uL
                        }
                    processes.Add (pid,v)
                    reg(fun () -> pco.on_create v) ct.Token
                ) ct.Token)
                pid
            member x.get_all() = processes.Keys|>Seq.toArray
            member x.get_pid i = processes.TryGetValue(i)|>Option.ofBoolObjTuple
            member x.kill(i) = 
                lock processes (fun() -> lock cts (fun () ->
                    match cts.TryGetValue(i) with
                    |true,v -> v();processes.Remove(i)|>ignore
                    |_ -> ()
                ))
            member x.call p =
                match p with
                |{call = 0u; arg = n} -> 
                    let n = unbox n
                    lock srs (fun() -> 
                        match srs.TryGetValue n with 
                        |true,v -> box v 
                        |_ -> let sr = getsr(n) in srs.Add(n,sr); box sr
                    )
                |{call = 1u; arg = parent} ->
                    let parent = unbox parent
                    let pid = nextpid() 
                    let ct = new System.Threading.CancellationTokenSource()
                    let pco' = processes.[parent].pco
                    lock cts (fun () -> cts.Add(pid,ct.Cancel))
                    (reg(fun() ->
                        let pco = ProcessCreationOptions.Express(pco',x.call)
                        let v = 
                            {
                                pid = pid
                                channel = SigChannel(function
                                        |Sig.KILL -> 
                                            ct.Cancel()
                                            processes.Remove(pid)|>ignore
                                            cts.Remove(pid)|>ignore
                                        |s -> reg(fun() -> pco.channel(s)) ct.Token)
                                get_state = pco.get_state
                                name = pco.name
                                pco = pco'
                                parent = parent
                            }
                        processes.Add (pid,v)
                        reg(fun () -> pco.on_create v) ct.Token
                    ) ct.Token)
                    pid|>box
                |_ -> box()
        }
    let SafeLocalProcessor(ts) = 
        let srs = new System.Collections.Generic.Dictionary<uint64,Comm.bsr>()
        let reg f (ct:System.Threading.CancellationToken) = 
            let v = System.Threading.Thread(System.Threading.ThreadStart(f))
            ct.Register(new System.Action(v.Abort))|>ignore
            v.Start()
        let processes = new System.Collections.Generic.Dictionary<uint64, Process>()
        let nextpid =
            let pid = ref 1uL
            let rec inner() = if processes.ContainsKey(!pid) then pid := !pid + 1uL;inner() else !pid
            fun() -> lock pid inner
        let cts = new System.Collections.Generic.Dictionary<uint64, (unit -> unit)>()
        {new IProcessor with
            member x.start(pco') = 
                let pid = nextpid() 
                let ct = new System.Threading.CancellationTokenSource()
                lock cts (fun () -> cts.Add(pid,ct.Cancel))
                (reg(fun() ->
                    let pco = ProcessCreationOptions.Express(pco',ts,x.call)
                    let v = 
                        {
                            pid = pid
                            channel = SigChannel(function
                                    |Sig.KILL -> 
                                        ct.Cancel()
                                        processes.Remove(pid)|>ignore
                                        cts.Remove(pid)|>ignore
                                    |s -> reg(fun() -> pco.channel(s)) ct.Token)
                            get_state = pco.get_state
                            name = pco.name
                            pco = pco'
                            parent = 0uL
                        }
                    processes.Add (pid,v)
                    reg(fun () -> pco.on_create v) ct.Token
                ) ct.Token)
                pid
            member x.get_all() = processes.Keys|>Seq.toArray
            member x.get_pid i = processes.TryGetValue(i)|>Option.ofBoolObjTuple
            member x.kill(i) = 
                lock processes (fun() -> lock cts (fun () ->
                    match cts.TryGetValue(i) with
                    |true,v -> v();processes.Remove(i)|>ignore
                    |_ -> ()
                ))
            member x.call p =
                match p with
                |{call = 0u; arg = n} -> 
                    let n = unbox n
                    match srs.TryGetValue n with 
                    |true,v -> box v 
                    |_ -> 
                        let sr = Comm.loopback<byte[]>()
                        srs.Add(n,sr);box sr
                |_ -> box()
        }
    let RemoteProcessor(sr:Comm.bbsr) =
        let b (v:RemoteCommunication) = v.GetBytes()
        let bc() = raise BadCommunication
        let ask (r:RemoteCommunication) = 
            let g = Net.Protocols.GPP(randb 8 |> _b_uint64)
            let v = r.GetBytes()|>g.sign
            sr.Send(v)
            sr.TakeMap g.verify |> RemoteCommunication.OfBytes
        {new IProcessor with
            member x.start(pco) = 
                let id = randb 8
                Start(pco)|>b|>Array.append id|>sr.Send
                let p = match sr.Take(fun b -> b.[..7]=id).[8..]|> RemoteCommunication.OfBytes with |Started v -> v |_ -> bc()
                p
            member x.get_all() = match ask GetAll with |GotAll(v) -> v |_ -> bc()
            member x.get_pid pid = 
                match ask(GetName pid) with 
                |GotName(s) -> 
                    {
                        pid = pid
                        channel = SigChannel(fun i -> SetChannel(pid,i) |> b |> Array.append (Array.zeroCreate 8) |> sr.Send)
                        get_state = (fun() -> match GetState pid |> ask with |GotState(s) -> s |_ -> bc())
                        name = s
                        pco = match ask(GetPCO pid) with |GotPCO(p) -> p |_ -> bc()
                        parent = match ask(GetParent pid) with |GotParent(p) -> p |_ -> bc()
                    }
                    |> Some
                |_ -> None
            member x.kill i = SetChannel(i,Sig.KILL)|>b|>sr.Send
            member x.call p = 
                match p with 
                |{call = 0u;arg = (i)} -> 
                    let g = Net.Protocols.GPP(unbox i)
                    ({Comm.send = Msg >> b >> g.sign >> sr.Send;Comm.receive = fun() -> sr.TakeMap(g.verify)|>Array.skip 1} : Comm.bsr)|>box
                |_ -> box()
        }
    
    let RemoteProcessorHandler(sr:Comm.bsr) = 
        async{
            let f (v:RemoteCommunication) = v.GetBytes()
            let msg = new System.Collections.Generic.Dictionary<uint64,System.Collections.Generic.Queue<byte[]>>()
            let p = LocalProcessor(Some(fun i -> 
                let i' = _uint64_b i
                msg.Add(i,System.Collections.Generic.Queue())
                {Comm.send = Msg >> f >> Array.append i' >> sr.send;Comm.receive = msg.[i].Dequeue >> side (printfn "!%A") >> Array.skip 1}))
            while true do 
                let a,b = sr.receive()|>Array.splitAt 8
                match RemoteCommunication.OfBytes(b) with
                |Msg(c) -> (p.call(get_sr(_b_uint64 a)):?>Comm.bsr).send c
                |Start(c) -> c |> p.start |> Started |> f |> Array.append a |> sr.send
                |SetChannel(i,c) -> i |> p.get_pid |> Option.iter (fun v -> v.channel.Send c)
                |GetState(i) -> 
                    i
                    |> p.get_pid
                    |> (function |Some(v) -> v.get_state() |None -> Sig.NONEXIST)
                    |> GotState
                    |> f
                    |> Array.append a
                    |> sr.send
                |GetName(i) -> i |> p.get_pid |> (function |Some(v) -> v.name |> GotName |None -> No) |> f |>Array.append a|>sr.send
                |GetPCO(i) -> i |> p.get_pid |> (function |Some(v) -> v.pco |> GotPCO |None -> No) |> f |>Array.append a|>sr.send
                |GetParent(i) -> i |> p.get_pid |> (function |Some(v) -> v.parent |> GotParent |None -> No) |> f |>Array.append a|>sr.send
                |_ -> raise BadCommunication
        }
    let SafeRemoteProcessorHandler(sr:Comm.bsr,ct) = 
        async{
            let p = SafeLocalProcessor(ct)
            let f (v:RemoteCommunication) = v.GetBytes()
            while true do 
                let a,b = sr.receive()|>Array.splitAt 8
                match RemoteCommunication.OfBytes(b) with
                |Msg(b) ->  (p.call(get_sr(_b_uint64 a)):?>Comm.bsr).send b
                |Start(c) -> c |> p.start |> Started |> f |> Array.append a |> sr.send
                |SetChannel(i,c) -> i |> p.get_pid |> Option.iter (fun v -> v.channel.Send c)
                |GetState(i) -> 
                    i
                    |> p.get_pid
                    |> (function |Some(v) -> v.get_state() |None -> Sig.NONEXIST)
                    |> GotState
                    |> f
                    |> Array.append a
                    |> sr.send
                |GetName(i) -> i |> p.get_pid |> (function |Some(v) -> v.name|> GotName |None -> No) |> f |>Array.append a|>sr.send
                |_ -> raise BadCommunication
        }