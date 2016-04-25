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
    ///A discrimkinated union for checking quotation expressions
    type CheckType = 
        |AllowType of System.Type
        |AllowNamespace of System.Type
        |AllowIf of (System.Type -> bool)

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

    ///A channel for sending signals
    type SigChannel(signalHandler : Sig -> unit) = member x.Send (s:Sig) = signalHandler s

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
        static member Express(v) = 
            let rec outer a (d:System.Collections.Generic.Dictionary<Var,obj>)= 
                let rec inner = function
                    |Patterns.Value(v,_) -> v
                    |Patterns.Var(v) -> d.[v]
                    |Patterns.VarSet(v,e) -> d.[v] <- (inner e);box()
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
                    |e -> failwithf "%A" e
                inner a
            outer v (new System.Collections.Generic.Dictionary<Var,obj>()) |> unbox<ProcessCreationOptions>
        static member UnMash(b:byte[]) = using(new System.IO.MemoryStream(b)) (System.Runtime.Serialization.Formatters.Binary.BinaryFormatter().Deserialize>>unbox)

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
                        channel = SigChannel(fun s -> reg(fun()-> pco.channel(s)) ct.Token)
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
    type RemoteCommunication = 
        |Start of uint64 * Expr<ProcessCreationOptions>
        |Started of uint64
        |SetChannel of Sig
        member x.GetBytes() = 
            match x with
            |Start(i,v) -> Array.concat [|[|0uy|];(_uint64_b i);ProcessCreationOptions.Mash v|]
            |Started(i) -> Array.append [|1uy|] (_uint64_b i)
        static member OfBytes(b':byte[]) = 
            let b = b'.[1..]
            match b'.[0] with
            |0uy -> Start(_b_uint64 b, ProcessCreationOptions.UnMash b.[7..])
            |1uy -> Started(_b_uint64 b)
            |_ -> raise(MatchFailureException())
    let RemoteProcessor(sr:Comm.BufferedSendReceive<RemoteCommunication>) =
        let processes = new System.Collections.Generic.Dictionary<uint64, Process>()
        let nextpid =
            let pid = ref 0uL
            let rec inner() = if processes.ContainsKey(!pid) then pid := !pid + 1uL;inner() else !pid
            fun() -> lock pid inner
        {new IProcessor<Expr<ProcessCreationOptions>> with
            member x.start(pco) = 
                Start(nextpid(),pco) |> sr.Send
                let pid = sr.Take(function |Started _ -> true |_ -> false)
                {pid = pid; channel = SetChannel |> sr.Send }
        }
    
    let f() =
        (System.Reflection.MethodInfo.GetCurrentMethod()).GetMethodBody().GetILAsByteArray()
