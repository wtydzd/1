#r "nuget: Akka.FSharp" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

type NodeMetaInfo = {
        mutable NodeId: int
        mutable NodeInstance: IActorRef
    }

type RingMasterMessage = 
    | FindSuccessor of int
    | JoinRing of int * bool
    | StabilizeRing
    | StartSearch
    | InitializeFingerTable
    | InitializeRing of Dictionary<int,IActorRef>
    | DistributeKeys of list<int>
    | CountSearches
    | CountHops
    | GetRingList

type RingWorkerMessage =
    | AssignId of int
    | InitKeyList of int
    | SetSuccessor of int * IActorRef
    | SetPrev of int * IActorRef
    | CreateFingerTable of list<int> * Dictionary<int, IActorRef> * IActorRef
    | StabilizeNodeReq
    | RequestPrev of IActorRef
    | NodeNotify of NodeMetaInfo
    | KeyShare of list<int> * IActorRef
    | SetKeys of list<int>
    | KeySearch of int * IActorRef * IActorRef
    | KeyFound of int * int * IActorRef
    | DisplayRing

let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequestsPerNode = fsi.CommandLineArgs.[2] |> int
let stopWatch = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")
let rand = Random()

if numNodes <= 0 || numRequestsPerNode <= 0 then
    printfn "Invalid input"
    Environment.Exit(0)

let retrieveSuccessorFromDict (currentId:int, nodesDict:Dictionary<int,_>) =
    let mutable found = false
    let mutable nextNode = 0
    if currentId < numNodes then
        for nodeId in (currentId + 1) .. numNodes do 
            if nodesDict.ContainsKey(nodeId) && not found then
                nextNode <- nodeId
                found <- true
    nextNode 

let retrieveSuccessorFromList (currentId:int, nodesList:list<int>) =
    try
        nodesList 
        |> List.sort 
        |> List.tryFind (fun node -> node > currentId)
        |> function
            | Some value -> value
            | None -> 0
    with 
        | :? System.Collections.Generic.KeyNotFoundException -> 0

let findInFingerTable (searchKey:int, table:list<int>) =
    let mutable closestKey = 0
    for nodeId in table do 
        if searchKey >= nodeId && nodeId >= closestKey then 
            closestKey <- nodeId
    closestKey

let RandomJoin(maxNodes:int, globalNodesDict: Dictionary<int, IActorRef>, nodeList:ResizeArray<int>, master:IActorRef) = 
    for x in [1..maxNodes] do
        let rndNodeId = Random().Next(1,nodeList.Count)
        let worker = globalNodesDict.[nodeList.[rndNodeId]] 
        let response =  (master <? FindSuccessor nodeList.[rndNodeId])
        let successorId = Async.RunSynchronously (response, 10000)
        worker <! JoinRing successorId
        nodeList.RemoveAt(rndNodeId) |> ignore

let fingerTableSize = 20 // Set the value of m to 20

let RingMaster(mailbox: Actor<_>) =
    
    let mutable searchCount = 0
    let mutable hopCount = 0
    let mutable localNodeList = []
    let mutable nodeSaturationCount = 0
    let mutable globalNodesDict = new Dictionary<int,IActorRef>()

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        let response = mailbox.Sender();
        try
            match msg with 
                | InitializeRing nodeDict ->
                    printfn "Initializing..."
                    globalNodesDict <- nodeDict
 
                | JoinRing (nodeId, selfjoin) ->
                    if nodeId > 0 then
                        if not selfjoin then
                            let successorId = retrieveSuccessorFromList(nodeId, localNodeList)
                            globalNodesDict.[nodeId] <! SetSuccessor (successorId, globalNodesDict.[successorId])
                        localNodeList <- localNodeList @ [nodeId]

                | GetRingList ->
                    response <! localNodeList

                | CountSearches ->
                    searchCount <- searchCount + 1
                    if searchCount = (numNodes * numRequestsPerNode) then
                        stopWatch.Stop()
                        let avgHopCount = (hopCount |> double)/((numNodes*numRequestsPerNode) |> double)
                        printfn "Average number of hops = %f" avgHopCount
                        Environment.Exit(0)

                | CountHops ->
                    hopCount <- hopCount + 1
                
                | StabilizeRing ->
                    for KeyValue(key, worker) in globalNodesDict do
                        if key = 0 then
                            let successorId = retrieveSuccessorFromList(0, localNodeList)
                            globalNodesDict.[0] <! SetSuccessor (successorId, globalNodesDict.[successorId])
                        worker <! StabilizeNodeReq
    
                    let delay = async { do! Async.Sleep(5000) }
                    Async.RunSynchronously(delay)
                    mailbox.Self <! StabilizeRing

                | InitializeFingerTable ->
                    for KeyValue(key, worker) in globalNodesDict do
                        worker <! CreateFingerTable (localNodeList, globalNodesDict, mailbox.Self)

                | DistributeKeys keyslist ->
                    let avgKeys = keyslist.Length / localNodeList.Length
                    let mutable updatedKeyList = ( keyslist |> List.sort)
                    let reverseNodeList = ( localNodeList |> List.sortDescending ) 
                    for nodeId in reverseNodeList do
                        mailbox.Self <! StartSearch 
                        let nodeKeyList = ( updatedKeyList |> List.filter ( fun(key) -> (key % numNodes) = nodeId )  )
                        updatedKeyList <- ( updatedKeyList |> List.except nodeKeyList)
                        globalNodesDict.[nodeId] <! SetKeys nodeKeyList

                | StartSearch -> 
                    nodeSaturationCount <- nodeSaturationCount + 1
                    localNodeList <- List.sort localNodeList
                    if nodeSaturationCount = 2 * numNodes then
                        printfn "Start searching..."
                        for KeyValue(key, worker) in globalNodesDict do
                            for numKeys in [1 .. numRequestsPerNode] do
                                let randomKey = rand.Next(1, (numNodes * numRequestsPerNode))
                                // System.Threading.Thread.Sleep(5000)
                                worker <! KeySearch(randomKey, worker, mailbox.Self) 

                | _ -> ()
        with
            | :? System.IndexOutOfRangeException -> printfn "ERROR: Tried to access outside array!" |> ignore
        return! loop()
    }            
    loop()

let RingWorker (mailbox: Actor<_>) =
    let mutable currentId = -1
    let mutable nextNode = {NodeId = -1; NodeInstance = null}
    let mutable prevNode = {NodeId = -1; NodeInstance = null}
    let mutable localKeys = []
    let mutable fingerTbl = new Dictionary<int, IActorRef>()

    let rec loop()= actor{
        let! message = mailbox.Receive();
        let response = mailbox.Sender();
        match message with
            | AssignId identifier -> currentId <- identifier
            | SetSuccessor(currentId, ref) -> 
                if currentId <> -1 then
                    try 
                        nextNode.NodeId <- currentId
                        nextNode.NodeInstance <- ref
                        nextNode.NodeInstance <! SetPrev (currentId, mailbox.Self)
                    with _ -> printfn "ERROR: Key missing" |> ignore
            | RequestPrev req -> req <! NodeNotify prevNode
            | SetPrev (pId, pRef) ->
                prevNode.NodeId <- pId
                prevNode.NodeInstance <- pRef
            | StabilizeNodeReq -> nextNode.NodeInstance <! RequestPrev mailbox.Self
            | NodeNotify nPredecessor  ->
                if (nPredecessor.NodeId <> -1) && (nPredecessor.NodeId > currentId) then
                    nextNode.NodeId <- nPredecessor.NodeId
                    nextNode.NodeInstance <- nPredecessor.NodeInstance 
                    nextNode.NodeInstance <! SetPrev (currentId, mailbox.Self)
            | CreateFingerTable (nodeList, globalDict, master) ->
                for exp in [0..fingerTableSize] do
                    let mutable nextEntry = currentId + (pown 2 exp) - 1
                    if (nextEntry <= numNodes) then
                        let sId = retrieveSuccessorFromList  (nextEntry, nodeList)
                        fingerTbl.Add(sId, globalDict.[sId])
                if not (fingerTbl.ContainsKey(0)) && currentId <> 0 then 
                    fingerTbl.Add(0, globalDict.[0])
                master <! StartSearch
            | SetKeys keyUpdate ->
                localKeys <- localKeys @ keyUpdate

            | KeyShare (allKeys, primaryRef) ->
                primaryRef <! StartSearch
                let mutable newKeyBatch = []
                for singleKey in allKeys do
                    if (singleKey % numNodes) >= currentId then
                        localKeys <- localKeys @ [singleKey]
                    else    
                        newKeyBatch <- newKeyBatch @ [singleKey]

                if newKeyBatch.Length > 0 then
                    if prevNode.NodeId <> -1 then
                        prevNode.NodeInstance <! KeyShare (newKeyBatch, primaryRef)
                    else 
                        printfn "INFO: Disconnected at %i stabilising" currentId
                        mailbox.Self <! StabilizeNodeReq
                        mailbox.Self <! KeyShare (newKeyBatch, primaryRef)

            | KeySearch (searchKey, reqRef, primaryRef) ->
                primaryRef <! CountHops 
                let mutable found = false
                for k in localKeys do
                    if searchKey = k then
                        found <- true
                        reqRef <! KeyFound (searchKey, currentId, primaryRef)
                    
                if not found then 
                    if fingerTbl.Count < 1 then nextNode.NodeInstance <! KeySearch (searchKey, reqRef, primaryRef)
                    else 
                        let tblKeys = [ for KeyValue(key, _) in fingerTbl do yield key ]
                        let targetNode =  findInFingerTable(searchKey % numNodes, tblKeys)
                        fingerTbl.[targetNode] <! KeySearch (searchKey, reqRef, primaryRef)

            | KeyFound (foundKey, finderId, primaryRef) ->
                printfn "Found key %i at node %i" foundKey finderId
                primaryRef <! CountSearches

            | DisplayRing ->
                printf "%i --->>> " currentId
                nextNode.NodeInstance <! DisplayRing

            | _ -> ()
        return! loop()
    }   
    loop()

stopWatch.Start()
let mutable globalNodesDict = new Dictionary<int,IActorRef>()
let master = spawn system "Master" RingMaster

for nodeId in [0 .. numNodes] do
    let key: string = "RingWorker" + string(nodeId)
    let worker = spawn system (key) RingWorker
    worker <! AssignId nodeId
    globalNodesDict.Add(nodeId, worker)

master <! InitializeRing globalNodesDict
master <! JoinRing (0, false)

for nodeId in [numNodes .. -1 .. 1] do
    let nodesList = Async.RunSynchronously(master <? GetRingList)
    let successorId = retrieveSuccessorFromList(nodeId, nodesList)
    globalNodesDict.[nodeId] <! SetSuccessor (successorId, globalNodesDict.[successorId])
    master <! JoinRing (nodeId, true)

master <! StabilizeRing

let keysList = [0 .. (numNodes * numRequestsPerNode)]
master <! DistributeKeys keysList

master <! InitializeFingerTable

Console.ReadLine() |> ignore

system.WhenTerminated.Wait()