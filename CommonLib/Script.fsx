open Microsoft.FSharp.Quotations
#r "bin/Debug/CommonLib.dll"
open C_Omega.Delta.Processes
System.IO.File.WriteAllBytes("/home/harlan/C-Omega/1.tmp",ProcessCreationOptions.Mash <@ {channel = printfn "%A";name = "test";get_state = (fun() -> Sig.START); on_create = ignore} @> )
ProcessCreationOptions.UnMash(System.IO.File.ReadAllBytes("/home/harlan/C-Omega/1.tmp"),[||])