namespace C_Omega.Delta
open C_Omega
open C_Omega.Helpers
open C_Omega.ArraySliceImprovement
module Extensions = 
    module Option = 
        let ofBoolObjTuple (a,b) = if a then Some(b) else None
module Processes = 
    open Microsoft.FSharp.Quotations
    open Extensions
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

    ///A record type cotaining the pid, channel and state of a process
    type Process = 
        {
            pid : uint64
            channel : SigChannel
            get_state : unit -> Sig
            name : string
        }
    ///Options required to create a process
    type ProcessCreationOptions = 
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
        static member Express(v,vars:seq<Var*obj>) = 
            let rec outer a (d:System.Collections.Generic.Dictionary<Var,obj>)= 
                let rec inner = function
                    |Patterns.Value(v,t) -> v
                    |Patterns.Var(v) -> d.[v]
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
                    |e -> failwithf "%A" e
                inner a
            outer v (new System.Collections.Generic.Dictionary<Var,obj>(dict vars)) |> unbox<ProcessCreationOptions>
        static member Express(v,ct,vars) = 
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
            outer v (new System.Collections.Generic.Dictionary<Var,obj>(dict vars)) |> unbox<ProcessCreationOptions>
        static member UnMash(b:byte[]) = printfn "%A" b;using(new System.IO.MemoryStream(b)) (System.Runtime.Serialization.Formatters.Binary.BinaryFormatter().Deserialize>>unbox)
    type ProcessorCall = {call : uint32; arr : nativeint}
    ///An interface that represents a processor.
    type IProcessor<'a> = 
        abstract start : 'a -> Process
        abstract get_all : unit -> Process[]
        abstract get_pid : uint64 -> Process option
        abstract kill : uint64 -> unit

    let LocalProcessor() =
        let reg f (ct:System.Threading.CancellationToken) = 
            let v = System.Threading.Thread(System.Threading.ThreadStart(f))
            ct.Register(new System.Action(v.Abort))|>ignore
            v.Start()
        let processes = new System.Collections.Generic.Dictionary<uint64, Process>()
        let nextpid =
            let pid = ref 0uL
            let rec inner() = if processes.ContainsKey(!pid) then pid := !pid + 1uL;inner() else !pid
            fun() -> lock pid inner
        let cts = new System.Collections.Generic.Dictionary<uint64, (unit -> unit)>()
        {new IProcessor<ProcessCreationOptions> with
            member x.start(pco) = 
                let pid = nextpid() 
                let ct = new System.Threading.CancellationTokenSource()
                lock cts (fun () -> cts.Add(pid,ct.Cancel))
                let v = 
                    {
                        pid = pid
                        channel = SigChannel(function
                                |Sig.KILL -> 
                                    ct.Cancel()
                                    processes.Remove(pid)|>ignore
                                    cts.Remove(pid)|>ignore
                                |s -> reg(fun()->pco.channel(s)) ct.Token)
                        get_state = pco.get_state
                        name = pco.name
                    }
                lock processes (fun() -> processes.Add (pid,v))
                reg(fun () -> pco.on_create v) ct.Token
                v
            member x.get_all() = processes.Values|>Seq.toArray
            member x.get_pid i = processes.TryGetValue(i)|>Option.ofBoolObjTuple
            member x.kill(i) = 
                lock processes (fun() -> lock cts (fun () ->
                    match cts.TryGetValue(i) with
                    |true,v -> v();processes.Remove(i)|>ignore
                    |_ -> ()
                ))
        }
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
        |Msg of uint64*byte[]
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
            |Msg(i,b) -> Array.concat [|[|9uy|];_uint64_b i;b|]
            |No -> [|255uy|]
        static member OfBytes(b':byte[]) = 
            match b'.[0],if b'.Length > 1 then Some(b'.[1..]) else None with
            |0uy,Some(b) -> Start(ProcessCreationOptions.UnMash b)
            |1uy,Some(b) -> Started(_b_uint64 b)
            |2uy,Some(b) -> SetChannel(_b_uint64 b.[1..],enum b.[0])
            |3uy,Some(b) -> GetState(_b_uint64 b)
            |4uy,Some(b) -> GotState(Array.head b |> enum)
            |5uy,Some(b) -> GetName(_b_uint64 b)
            |6uy,Some(b) -> GotName(System.Text.ASCIIEncoding.UTF8.GetString(b))
            |7uy,None    -> GetAll
            |8uy,Some(b) -> b |>Array.chunkBySize 8 |> Array.map _b_uint64 |> GotAll
            |9uy,Some(b) -> Msg(_b_uint64 b,b.[8..])
            |255uy, None -> No
            |_ -> raise BadCommunication
    let RemoteProcessor(sr:Comm.bbsr) =
        let b (v:RemoteCommunication) = v.GetBytes()
        let bc() = raise BadCommunication
        let ask (r:RemoteCommunication) = 
            let g = Net.Protocols.GPP(randb 8 |> _b_uint64)
            let v = r.GetBytes()|>g.sign
            sr.Send(v)
            sr.TakeMap g.verify |> RemoteCommunication.OfBytes
        let get pid = 
            {
                pid = pid
                channel = SigChannel(fun i -> SetChannel(pid,i) |> b |> Array.append (Array.zeroCreate 8) |> sr.Send)
                get_state = (fun() -> match GetState pid |> ask with |GotState(s) -> s |_ -> bc())
                name = match GetName pid |> ask with |GotName s -> s |_ -> bc()
            }
        {new IProcessor<Expr<ProcessCreationOptions>> with
            member x.start(pco) = 
                let id = randb 8
                Start(pco)|>b|>Array.append id|>sr.Send
                let p = match sr.Take(fun b -> b.[..7]=id).[8..]|> RemoteCommunication.OfBytes with |Started v -> v |_ -> bc()
                let p' = get p
                p'
            member x.get_all() =
                match ask GetAll with |GotAll(v) -> v |_ -> bc()
                |> Array.map get
            member x.get_pid pid = 
                match ask(GetName pid) with 
                |GotName(s) -> 
                    {
                        pid = pid
                        channel = SigChannel(fun i -> SetChannel(pid,i) |> b |> sr.Send)
                        get_state = (fun() -> match GetState pid |> ask with |GotState(s) -> s |_ -> bc())
                        name = s
                    } |> Some
                |_ -> None
            member x.kill i = SetChannel(i,Sig.KILL)|>b|>sr.Send
        }
    let sendreceive = Var("sendreceive",typeof<Comm.bsr>)
    let RemoteProcessorHandler(sr:Comm.bsr) = 
        async{
            let p = LocalProcessor()
            let f (v:RemoteCommunication) = v.GetBytes()
            let bs = System.Collections.Generic.Dictionary<uint64,ResizeArray<byte[]>>()
            while true do 
                let a,b = sr.receive()|>Array.splitAt 8
                match RemoteCommunication.OfBytes(b) with
                |Msg(i,b) -> bs.[i].Add b
                |Start(c) -> 
                    let n = _b_uint64 a
                    let sr' = {
                        Comm.send = fun v -> Msg(n,v)|>f|>sr.send; 
                        Comm.receive = fun() -> let v = bs.[n] in lock v (fun() -> let a = v.[0] in v.RemoveAt 0; a)
                    }
                    (ProcessCreationOptions.Express(c,[sendreceive,box sr'])|>p.start).pid|>Started|>f|>Array.append a|>sr.send
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
    let SafeRemoteProcessorHandler(sr:Comm.bsr,ct) = 
        async{
            let p = LocalProcessor()
            let f (v:RemoteCommunication) = v.GetBytes()
            while true do 
                let a,b = sr.receive()|>Array.splitAt 8
                match RemoteCommunication.OfBytes(b) with
                |Start(c) -> (ProcessCreationOptions.Express (c,ct)|>p.start).pid|>Started|>f|>Array.append a|>sr.send
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