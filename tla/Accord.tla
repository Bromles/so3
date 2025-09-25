---------------------------- MODULE Accord ----------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC, Naturals

CONSTANTS 
    Replicas,          \* Set of replica IDs
    Transactions,      \* Set of transaction IDs  
    Keys,              \* Set of keys that transactions can access
    Quorum,            \* Quorum size (majority)
    FastQuorum,        \* Fast path quorum size
    Coordinators,      \* Set of coordinator IDs
    MaxBallot,         \* Maximum ballot number
    MaxEpoch,          \* Maximum epoch number
    MaxTime            \* Maximum timestamp time value

ASSUME /\ Quorum = (Cardinality(Replicas) \div 2) + 1
       /\ FastQuorum >= Quorum
       /\ FastQuorum <= Cardinality(Replicas)

\* Message types
MessageType == {"PreAccept", "PreAcceptOK", "Accept", "AcceptOK", 
                "Commit", "Read", "ReadOK", "Apply", 
                "Recover", "RecoverOK"}

\* Transaction states
TxnState == {"init", "preaccepted", "accepted", "committed", "executed", "applied"}

\* Coordinator states
CoordState == {"init", "preaccept", "accept", "committed", "reading", "applying", "done"}

\* Helper function to convert node ID to integer for comparison
NodeToInt(node) ==
    CASE node = "r1" -> 1
      [] node = "r2" -> 2
      [] node = "r3" -> 3
      [] node = "c1" -> 10
      [] node = "R1" -> 20
      [] node = "R2" -> 21
      [] OTHER -> 99
      
NodeToInt2(node) ==
    IF node \in {Replicas[i] : i \in DOMAIN Replicas} THEN
        CHOOSE i \in DOMAIN Replicas : Replicas[i] = node
    ELSE IF node \in {Transactions[i] : i \in DOMAIN Transactions} THEN
        Cardinality(Replicas) +
        CHOOSE i \in DOMAIN Transactions : Transactions[i] = node
    ELSE IF node \in {Keys[i] : i \in DOMAIN Keys} THEN
        Cardinality(Replicas) + Cardinality(Transactions) +
        CHOOSE i \in DOMAIN Keys : Keys[i] = node
    ELSE IF node \in {Coordinators[i] : i \in DOMAIN Coordinators} THEN
        Cardinality(Replicas) + Cardinality(Transactions) + Cardinality(Keys) +
        CHOOSE i \in DOMAIN Coordinators : Coordinators[i] = node
    ELSE
        99999

\* Helper functions
CompareTimestamp(ts1, ts2) ==
    IF ts1.epoch # ts2.epoch THEN ts1.epoch < ts2.epoch
    ELSE IF ts1.time # ts2.time THEN ts1.time < ts2.time  
    ELSE IF ts1.seq # ts2.seq THEN ts1.seq < ts2.seq
    ELSE NodeToInt(ts1.node) < NodeToInt(ts2.node)

MaxTimestamp(ts1, ts2) ==
    IF CompareTimestamp(ts1, ts2) THEN ts2 ELSE ts1

CreateTimestamp(epoch, time, seq, node) ==
    [epoch |-> epoch, time |-> time, seq |-> seq, node |-> node]

IncrementTimestamp(ts, node) ==
    [epoch |-> ts.epoch, time |-> ts.time, seq |-> ts.seq + 1, node |-> node]

\* Check if a set of replicas forms a quorum
IsQuorum(S) == Cardinality(S) >= Quorum
IsFastQuorum(S) == Cardinality(S) >= FastQuorum

(* --algorithm Accord {

variables
    \* Replica state
    replicaState = [r \in Replicas |-> [t \in Transactions |-> "init"]];
    replicaTS = [r \in Replicas |-> [t \in Transactions |-> 
        CreateTimestamp(0, 0, 0, r)]];
    replicaDeps = [r \in Replicas |-> [t \in Transactions |-> {}]];
    replicaBallot = [r \in Replicas |-> [t \in Transactions |-> 0]];
    replicaAcceptBallot = [r \in Replicas |-> [t \in Transactions |-> -1]];
    maxTSForKey = [r \in Replicas |-> [k \in Keys |-> 
        CreateTimestamp(0, 0, 0, r)]];
    replicaEpoch = [r \in Replicas |-> 0];
    replicaResult = [r \in Replicas |-> [t \in Transactions |-> "none"]];
    replicaReads = [r \in Replicas |-> [t \in Transactions |-> {}]];
    
    \* Coordinator state
    coordState = [c \in Coordinators |-> [t \in Transactions |-> "init"]];
    coordTS = [c \in Coordinators |-> [t \in Transactions |-> 
        CreateTimestamp(0, 0, 0, c)]];
    coordDeps = [c \in Coordinators |-> [t \in Transactions |-> {}]];
    coordBallot = [c \in Coordinators |-> [t \in Transactions |-> 0]];
    coordResponses = [c \in Coordinators |-> [t \in Transactions |-> {}]];
    coordReads = [c \in Coordinators |-> [t \in Transactions |-> {}]];
    coordResult = [c \in Coordinators |-> [t \in Transactions |-> "none"]];
    
    \* Recovery state
    recState = [r \in {"R1", "R2"} |-> [t \in Transactions |-> "init"]];
    recBallot = [r \in {"R1", "R2"} |-> [t \in Transactions |-> 0]];
    recResponses = [r \in {"R1", "R2"} |-> [t \in Transactions |-> {}]];
    recTS = [r \in {"R1", "R2"} |-> [t \in Transactions |-> 
        CreateTimestamp(0, 0, 0, r)]];
    recDeps = [r \in {"R1", "R2"} |-> [t \in Transactions |-> {}]];
    
    \* Communication
    messages = {};
    
    \* Transaction specification
    txnKeys = [t1 |-> {"k1"}, t2 |-> {"k1", "k2"}];
    txnCoord = [t1 |-> "c1", t2 |-> "c1"];
    txnOps = [t1 |-> <<"read", "k1">>, t2 |-> <<"write", "k1", "v1">>];

define {
    ConflictingTxns(t1, t2) ==
        txnKeys[t1] \intersect txnKeys[t2] # {}
    
    TxnCommitted(t) ==
        \E r \in Replicas : replicaState[r][t] = "committed"
    
    TxnApplied(t) ==
        \E r \in Replicas : replicaState[r][t] = "applied"
    
    AllDepsCommitted(deps) ==
        \A d \in deps : TxnCommitted(d)
    
    AllLowerDepsApplied(deps, ts, r) ==
        \A d \in deps : 
            CompareTimestamp(replicaTS[r][d], ts) => 
                replicaState[r][d] = "applied"
    
    \* Invariants
    ConsistentTimestamps == 
        \A r1, r2 \in Replicas : \A t \in Transactions :
            (/\ replicaState[r1][t] \in {"committed", "executed", "applied"}
             /\ replicaState[r2][t] \in {"committed", "executed", "applied"}) =>
            replicaTS[r1][t] = replicaTS[r2][t]
    
    UniqueTimestamps ==
        \A r \in Replicas : \A t1, t2 \in Transactions :
            (/\ t1 # t2 
             /\ ConflictingTxns(t1, t2)
             /\ replicaState[r][t1] \in {"committed", "executed", "applied"}
             /\ replicaState[r][t2] \in {"committed", "executed", "applied"}) =>
            replicaTS[r][t1] # replicaTS[r][t2]
    
    DependencyConsistency ==
        \A r \in Replicas : \A t \in Transactions :
            (replicaState[r][t] \in {"committed", "executed", "applied"}) =>
            \A dep \in replicaDeps[r][t] :
                \/ ~ConflictingTxns(t, dep)
                \/ CompareTimestamp(replicaTS[r][dep], replicaTS[r][t])
    
    \* All conflicting transactions with lower timestamps are in dependencies
    CompleteDependencies ==
        \A r \in Replicas : \A t1, t2 \in Transactions :
            (/\ replicaState[r][t1] = "committed"
             /\ replicaState[r][t2] = "committed"
             /\ ConflictingTxns(t1, t2)
             /\ CompareTimestamp(replicaTS[r][t2], replicaTS[r][t1])) =>
            t2 \in replicaDeps[r][t1]
            
    \* Temporal properties
    EventuallyAllCommitted ==
        <>[](\A t \in Transactions : TxnCommitted(t))

    EventuallyAllApplied ==
        <>[](\A t \in Transactions : TxnApplied(t))

    \* Liveness under fairness
    Liveness == 
        \A t \in Transactions :
            (TxnCommitted(t) ~> TxnApplied(t))
}

\* Coordinator process
process (coordinator \in Coordinators)
variables
    currentTime = 1;
{
CoordinatorMain:
    while (TRUE) {
        either {
            \* Start a new transaction - PreAccept phase
            with (txn \in {t \in Transactions : 
                          /\ txnCoord[t] = self 
                          /\ coordState[self][t] = "init"}) {
                
                with (t0 = CreateTimestamp(0, currentTime, 0, self)) {
                    coordState[self][txn] := "preaccept";
                    coordTS[self][txn] := t0;
                    coordBallot[self][txn] := 0;
                    coordResponses[self][txn] := {};
                    coordDeps[self][txn] := {};
                    currentTime := currentTime + 1;
                    
                    messages := messages \union {[
                        type |-> "PreAccept",
                        txn |-> txn,
                        coord |-> self,
                        ballot |-> 0,
                        timestamp |-> t0,
                        keys |-> txnKeys[txn],
                        dest |-> r
                    ] : r \in Replicas};
                }
            }
        } or {
            \* Collect PreAccept responses and decide fast/slow path
            with (txn \in {t \in Transactions : 
                          /\ txnCoord[t] = self 
                          /\ coordState[self][t] = "preaccept"}) {
                
                await IsQuorum({r \in Replicas : 
                    \E msg \in messages : 
                        /\ msg.type = "PreAcceptOK"
                        /\ msg.txn = txn
                        /\ msg.coord = self
                        /\ msg.ballot = coordBallot[self][txn]
                        /\ msg.from = r});
                
                with (responses = {msg \in messages : 
                        /\ msg.type = "PreAcceptOK"
                        /\ msg.txn = txn
                        /\ msg.coord = self
                        /\ msg.ballot = coordBallot[self][txn]};
                      deps = UNION {msg.deps : msg \in responses};
                      t0 = coordTS[self][txn]) {
                    
                    coordResponses[self][txn] := responses;
                    coordDeps[self][txn] := deps;
                    
                    if (IsFastQuorum({msg.from : msg \in responses}) /\
                        \A msg \in responses : msg.timestamp = t0) {
                        \* Fast path success
                        coordState[self][txn] := "committed";
                        
                        messages := messages \union {[
                            type |-> "Commit",
                            txn |-> txn,
                            coord |-> self,
                            timestamp |-> t0,
                            deps |-> deps,
                            dest |-> r
                        ] : r \in Replicas};
                    } else {
                        \* Slow path - Accept phase
                        with (decidedTS = CHOOSE ts \in {msg.timestamp : msg \in responses} :
                                \A msg \in responses : ~CompareTimestamp(ts, msg.timestamp)) {
                            
                            coordState[self][txn] := "accept";
                            coordTS[self][txn] := decidedTS;
                            
                            messages := messages \union {[
                                type |-> "Accept",
                                txn |-> txn,
                                coord |-> self,
                                ballot |-> coordBallot[self][txn],
                                timestamp |-> decidedTS,
                                deps |-> deps,
                                dest |-> r
                            ] : r \in Replicas};
                        }
                    }
                }
            }
        } or {
            \* Collect Accept responses and commit
            with (txn \in {t \in Transactions : 
                          /\ txnCoord[t] = self 
                          /\ coordState[self][t] = "accept"}) {
                
                await IsQuorum({r \in Replicas :
                    \E msg \in messages :
                        /\ msg.type = "AcceptOK"
                        /\ msg.txn = txn
                        /\ msg.coord = self
                        /\ msg.ballot = coordBallot[self][txn]
                        /\ msg.from = r});
                
                with (acceptResponses = {msg \in messages :
                        /\ msg.type = "AcceptOK"
                        /\ msg.txn = txn
                        /\ msg.coord = self
                        /\ msg.ballot = coordBallot[self][txn]};
                      newDeps = UNION {msg.deps : msg \in acceptResponses}) {
                    
                    coordDeps[self][txn] := coordDeps[self][txn] \union newDeps;
                    coordState[self][txn] := "committed";
                    
                    messages := messages \union {[
                        type |-> "Commit",
                        txn |-> txn,
                        coord |-> self,
                        timestamp |-> coordTS[self][txn],
                        deps |-> coordDeps[self][txn],
                        dest |-> r
                    ] : r \in Replicas};
                }
            }
        } or {
            \* Execute phase - send Read requests
            with (txn \in {t \in Transactions :
                          /\ txnCoord[t] = self
                          /\ coordState[self][t] = "committed"}) {
                
                coordState[self][txn] := "reading";
                coordReads[self][txn] := {};
                
                \* Send Read to at least one replica per key
                messages := messages \union {[
                    type |-> "Read",
                    txn |-> txn,
                    coord |-> self,
                    timestamp |-> coordTS[self][txn],
                    deps |-> coordDeps[self][txn],
                    keys |-> txnKeys[txn],
                    dest |-> CHOOSE r \in Replicas : TRUE
                ]};
            }
        } or {
            \* Collect Read responses and apply
            with (txn \in {t \in Transactions :
                          /\ txnCoord[t] = self
                          /\ coordState[self][t] = "reading"}) {
                
                await \E msg \in messages :
                    /\ msg.type = "ReadOK"
                    /\ msg.txn = txn
                    /\ msg.coord = self;
                
                with (readResp = CHOOSE msg \in messages :
                        /\ msg.type = "ReadOK"
                        /\ msg.txn = txn
                        /\ msg.coord = self;
                      result = "executed_" \o txn) {  \* Simplified execution
                    
                    coordResult[self][txn] := result;
                    coordState[self][txn] := "applying";
                    
                    \* Send Apply to all replicas
                    messages := messages \union {[
                        type |-> "Apply",
                        txn |-> txn,
                        coord |-> self,
                        timestamp |-> coordTS[self][txn],
                        deps |-> coordDeps[self][txn],
                        result |-> result,
                        dest |-> r
                    ] : r \in Replicas};
                }
            }
        } or {
            \* Complete transaction
            with (txn \in {t \in Transactions :
                          /\ txnCoord[t] = self
                          /\ coordState[self][t] = "applying"}) {
                
                \* Wait for at least a quorum to apply
                await IsQuorum({r \in Replicas :
                    replicaState[r][txn] = "applied"});
                
                coordState[self][txn] := "done";
            }
        }
    }
}

\* Replica process
process (replica \in Replicas)
{
ReplicaMain:
    while (TRUE) {
        either {
            \* Handle PreAccept message
            with (msg \in {m \in messages : 
                /\ m.type = "PreAccept" 
                /\ m.dest = self}) {
                
                if (msg.ballot >= replicaBallot[self][msg.txn]) {
                    with (conflictKeys = msg.keys;
                          maxConflictTS = CHOOSE ts \in 
                            {maxTSForKey[self][k] : k \in conflictKeys} :
                            \A ts2 \in {maxTSForKey[self][k] : k \in conflictKeys} :
                                ~CompareTimestamp(ts, ts2);
                          proposedTS = IF ~CompareTimestamp(msg.timestamp, maxConflictTS)
                                       THEN msg.timestamp
                                       ELSE IncrementTimestamp(maxConflictTS, self);
                          localDeps = {t \in Transactions :
                            /\ t # msg.txn
                            /\ ConflictingTxns(t, msg.txn)
                            /\ replicaState[self][t] \in {"preaccepted", "accepted", "committed", "executed", "applied"}
                            /\ CompareTimestamp(replicaTS[self][t], msg.timestamp)}) {
                        
                        replicaBallot[self][msg.txn] := msg.ballot;
                        replicaState[self][msg.txn] := "preaccepted";
                        replicaTS[self][msg.txn] := proposedTS;
                        replicaDeps[self][msg.txn] := localDeps;
                        
                        \* Update max timestamp for keys
                        maxTSForKey[self] := [k \in Keys |->
                            IF k \in conflictKeys 
                            THEN MaxTimestamp(maxTSForKey[self][k], proposedTS)
                            ELSE maxTSForKey[self][k]];
                        
                        messages := messages \union {[
                            type |-> "PreAcceptOK",
                            txn |-> msg.txn,
                            coord |-> msg.coord,
                            ballot |-> msg.ballot,
                            from |-> self,
                            timestamp |-> proposedTS,
                            deps |-> localDeps
                        ]};
                    }
                }
            }
        } or {
            \* Handle Accept message
            with (msg \in {m \in messages :
                /\ m.type = "Accept"
                /\ m.dest = self}) {
                
                if (msg.ballot >= replicaBallot[self][msg.txn] /\
                    replicaState[self][msg.txn] # "committed") {
                    
                    with (localDeps = {t \in Transactions :
                            /\ t # msg.txn
                            /\ ConflictingTxns(t, msg.txn)
                            /\ replicaState[self][t] \in {"preaccepted", "accepted", "committed", "executed", "applied"}
                            /\ CompareTimestamp(replicaTS[self][t], msg.timestamp)}) {
                        
                        replicaBallot[self][msg.txn] := msg.ballot;
                        replicaAcceptBallot[self][msg.txn] := msg.ballot;
                        replicaState[self][msg.txn] := "accepted";
                        replicaTS[self][msg.txn] := msg.timestamp;
                        replicaDeps[self][msg.txn] := msg.deps;
                        
                        messages := messages \union {[
                            type |-> "AcceptOK",
                            txn |-> msg.txn,
                            coord |-> msg.coord,
                            ballot |-> msg.ballot,
                            from |-> self,
                            deps |-> localDeps
                        ]};
                    }
                }
            }
        } or {
            \* Handle Commit message
            with (msg \in {m \in messages :
                /\ m.type = "Commit"
                /\ m.dest = self}) {
                
                replicaState[self][msg.txn] := "committed";
                replicaTS[self][msg.txn] := msg.timestamp;
                replicaDeps[self][msg.txn] := msg.deps;
            }
        } or {
            \* Handle Read message
            with (msg \in {m \in messages :
                /\ m.type = "Read"
                /\ m.dest = self}) {
                
                \* Wait for dependencies to be committed
                if (AllDepsCommitted(msg.deps) /\
                    AllLowerDepsApplied(msg.deps, msg.timestamp, self)) {
                    
                    \* Simplified read - just return current state
                    replicaReads[self][msg.txn] := txnKeys[msg.txn];
                    
                    messages := messages \union {[
                        type |-> "ReadOK",
                        txn |-> msg.txn,
                        coord |-> msg.coord,
                        from |-> self,
                        reads |-> replicaReads[self][msg.txn]
                    ]};
                }
            }
        } or {
            \* Handle Apply message
            with (msg \in {m \in messages :
                /\ m.type = "Apply"
                /\ m.dest = self}) {
                
                \* Wait for dependencies to be applied
                if (AllDepsCommitted(msg.deps) /\
                    AllLowerDepsApplied(msg.deps, msg.timestamp, self)) {
                    
                    replicaState[self][msg.txn] := "applied";
                    replicaResult[self][msg.txn] := msg.result;
                }
            }
        } or {
            \* Handle Recover message
            with (msg \in {m \in messages :
                /\ m.type = "Recover"
                /\ m.dest = self}) {
                
                if (msg.ballot > replicaBallot[self][msg.txn]) {
                    replicaBallot[self][msg.txn] := msg.ballot;
                    
                    \* If not preaccepted yet, preaccept now
                    if (replicaState[self][msg.txn] = "init") {
                        with (conflictKeys = txnKeys[msg.txn];
                              maxConflictTS = CHOOSE ts \in 
                                {maxTSForKey[self][k] : k \in conflictKeys} :
                                \A ts2 \in {maxTSForKey[self][k] : k \in conflictKeys} :
                                    ~CompareTimestamp(ts, ts2);
                              proposedTS = IF ~CompareTimestamp(msg.timestamp, maxConflictTS)
                                           THEN msg.timestamp
                                           ELSE IncrementTimestamp(maxConflictTS, self)) {
                            
                            replicaState[self][msg.txn] := "preaccepted";
                            replicaTS[self][msg.txn] := proposedTS;
                            replicaDeps[self][msg.txn] := {};
                        }
                    };
                    
                    \* Prepare recovery response
                    with (superseding = {t \in Transactions :
                            /\ ConflictingTxns(t, msg.txn)
                            /\ msg.txn \notin replicaDeps[self][t]
                            /\ ((\/ replicaState[self][t] = "accepted" 
                                 /\ CompareTimestamp(msg.timestamp, replicaTS[self][t]))
                                \/ (replicaState[self][t] = "committed" 
                                 /\ CompareTimestamp(msg.timestamp, replicaTS[self][t])))};
                          waiting = {t \in Transactions :
                            /\ ConflictingTxns(t, msg.txn)
                            /\ replicaState[self][t] = "accepted"
                            /\ CompareTimestamp(replicaTS[self][t], msg.timestamp)
                            /\ CompareTimestamp(msg.timestamp, replicaTS[self][t])}) {
                        
                        messages := messages \union {[
                            type |-> "RecoverOK",
                            txn |-> msg.txn,
                            coord |-> msg.coord,
                            ballot |-> msg.ballot,
                            from |-> self,
                            state |-> replicaState[self][msg.txn],
                            timestamp |-> replicaTS[self][msg.txn],
                            deps |-> replicaDeps[self][msg.txn],
                            acceptBallot |-> replicaAcceptBallot[self][msg.txn],
                            superseding |-> superseding,
                            waiting |-> waiting
                        ]};
                    }
                }
            }
        }
    }
}

\* Recovery process
process (recovery \in {"R1", "R2"})
variables
    currentRecBallot = MaxBallot \div 2;
{
RecoveryMain:
    while (TRUE) {
        either {
            \* Start recovery for a transaction
            with (txn \in Transactions;
                  t0 = CHOOSE ts \in {CreateTimestamp(0, t, 0, "c1") : t \in 1..MaxTime} : TRUE) {
                
                \* Check if recovery is needed and we can get a higher ballot
                if (recState[self][txn] = "init" /\
                    currentRecBallot > 0 /\
                    \A r \in Replicas : currentRecBallot > replicaBallot[r][txn]) {
                    
                    recState[self][txn] := "recovering";
                    recBallot[self][txn] := currentRecBallot;
                    recResponses[self][txn] := {};
                    currentRecBallot := currentRecBallot + 1;
                    
                    messages := messages \union {[
                        type |-> "Recover",
                        txn |-> txn,
                        coord |-> self,
                        ballot |-> recBallot[self][txn],
                        timestamp |-> t0,
                        dest |-> r
                    ] : r \in Replicas};
                }
            }
        } or {
            \* Process recovery responses
            with (txn \in {t \in Transactions : recState[self][t] = "recovering"}) {
                
                await IsQuorum({r \in Replicas :
                    \E msg \in messages :
                        /\ msg.type = "RecoverOK"
                        /\ msg.txn = txn
                        /\ msg.coord = self
                        /\ msg.ballot = recBallot[self][txn]
                        /\ msg.from = r});
                
                with (responses = {msg \in messages :
                        /\ msg.type = "RecoverOK"
                        /\ msg.txn = txn
                        /\ msg.coord = self
                        /\ msg.ballot = recBallot[self][txn]}) {
                    
                    recResponses[self][txn] := responses;
                    
                    if (\E msg \in responses : msg.state = "applied") {
                        \* Already applied, nothing to do
                        recState[self][txn] := "done";
                        
                    } else if (\E msg \in responses : msg.state = "committed") {
                        \* Already committed, propagate
                        with (msg \in {m \in responses : m.state = "committed"}) {
                            recTS[self][txn] := msg.timestamp;
                            recDeps[self][txn] := msg.deps;
                            recState[self][txn] := "committing";
                            
                            messages := messages \union {[
                                type |-> "Commit",
                                txn |-> txn,
                                coord |-> self,
                                timestamp |-> msg.timestamp,
                                deps |-> msg.deps,
                                dest |-> r
                            ] : r \in Replicas};
                        }
                        
                    } else if (\E msg \in responses : msg.state = "accepted") {
                        \* Was accepted, use highest ballot's values
                        with (msg \in CHOOSE m \in {msg \in responses : msg.state = "accepted"} :
                                \A m2 \in {msg \in responses : msg.state = "accepted"} :
                                    m.acceptBallot >= m2.acceptBallot) {
                            recTS[self][txn] := msg.timestamp;
                            recDeps[self][txn] := msg.deps;
                            recState[self][txn] := "accepting";
                            
                            messages := messages \union {[
                                type |-> "Accept",
                                txn |-> txn,
                                coord |-> self,
                                ballot |-> recBallot[self][txn],
                                timestamp |-> msg.timestamp,
                                deps |-> msg.deps,
                                dest |-> r
                            ] : r \in Replicas};
                        }
                        
                    } else {
                        \* Only preaccepted, check if can recover fast path
                        with (fastPathVotes = Cardinality({msg \in responses : 
                                msg.timestamp = recTS[self][txn]});
                              superseding = UNION {msg.superseding : msg \in responses};
                              waiting = UNION {msg.waiting : msg \in responses}) {
                            
                            if (superseding # {} \/ waiting # {}) {
                                \* Cannot use t0, pick highest timestamp
                                with (highestTS = CHOOSE ts \in {msg.timestamp : msg \in responses} :
                                        \A msg \in responses : ~CompareTimestamp(ts, msg.timestamp);
                                      allDeps = UNION {msg.deps : msg \in responses}) {
                                    
                                    recTS[self][txn] := highestTS;
                                    recDeps[self][txn] := allDeps;
                                    recState[self][txn] := "accepting";
                                    
                                    messages := messages \union {[
                                        type |-> "Accept",
                                        txn |-> txn,
                                        coord |-> self,
                                        ballot |-> recBallot[self][txn],
                                        timestamp |-> highestTS,
                                        deps |-> allDeps,
                                        dest |-> r
                                    ] : r \in Replicas};
                                }
                            } else {
                                \* Can use t0
                                with (allDeps = UNION {msg.deps : msg \in responses}) {
                                    recTS[self][txn] := CreateTimestamp(0, 1, 0, "c1");  \* Use original t0
                                    recDeps[self][txn] := allDeps;
                                    recState[self][txn] := "accepting";
                                    
                                    messages := messages \union {[
                                        type |-> "Accept",
                                        txn |-> txn,
                                        coord |-> self,
                                        ballot |-> recBallot[self][txn],
                                        timestamp |-> recTS[self][txn],
                                        deps |-> allDeps,
                                        dest |-> r
                                    ] : r \in Replicas};
                                }
                            }
                        }
                    }
                }
            }
        } or {
            \* Complete Accept phase after recovery
            with (txn \in {t \in Transactions : recState[self][t] = "accepting"}) {
                
                await IsQuorum({r \in Replicas :
                    \E msg \in messages :
                        /\ msg.type = "AcceptOK"
                        /\ msg.txn = txn
                        /\ msg.coord = self
                        /\ msg.ballot = recBallot[self][txn]
                        /\ msg.from = r});
                
                with (acceptResponses = {msg \in messages :
                        /\ msg.type = "AcceptOK"
                        /\ msg.txn = txn
                        /\ msg.coord = self
                        /\ msg.ballot = recBallot[self][txn]};
                      newDeps = UNION {msg.deps : msg \in acceptResponses}) {
                    
                    recDeps[self][txn] := recDeps[self][txn] \union newDeps;
                    recState[self][txn] := "committing";
                    
                    messages := messages \union {[
                        type |-> "Commit",
                        txn |-> txn,
                        coord |-> self,
                        timestamp |-> recTS[self][txn],
                        deps |-> recDeps[self][txn],
                        dest |-> r
                    ] : r \in Replicas};
                }
            }
        } or {
            \* Complete recovery
            with (txn \in {t \in Transactions : 
                          recState[self][t] \in {"committing", "done"}}) {
                recState[self][txn] := "done";
            }
        }
    }
}

}
*)
\* BEGIN TRANSLATION (chksum(pcal) = "6170d634" /\ chksum(tla) = "589c8216")
VARIABLES replicaState, replicaTS, replicaDeps, replicaBallot, 
          replicaAcceptBallot, maxTSForKey, replicaEpoch, replicaResult, 
          replicaReads, coordState, coordTS, coordDeps, coordBallot, 
          coordResponses, coordReads, coordResult, recState, recBallot, 
          recResponses, recTS, recDeps, messages, txnKeys, txnCoord, txnOps

(* define statement *)
ConflictingTxns(t1, t2) ==
    txnKeys[t1] \intersect txnKeys[t2] # {}

TxnCommitted(t) ==
    \E r \in Replicas : replicaState[r][t] = "committed"

TxnApplied(t) ==
    \E r \in Replicas : replicaState[r][t] = "applied"

AllDepsCommitted(deps) ==
    \A d \in deps : TxnCommitted(d)

AllLowerDepsApplied(deps, ts, r) ==
    \A d \in deps :
        CompareTimestamp(replicaTS[r][d], ts) =>
            replicaState[r][d] = "applied"


ConsistentTimestamps ==
    \A r1, r2 \in Replicas : \A t \in Transactions :
        (/\ replicaState[r1][t] \in {"committed", "executed", "applied"}
         /\ replicaState[r2][t] \in {"committed", "executed", "applied"}) =>
        replicaTS[r1][t] = replicaTS[r2][t]

UniqueTimestamps ==
    \A r \in Replicas : \A t1, t2 \in Transactions :
        (/\ t1 # t2
         /\ ConflictingTxns(t1, t2)
         /\ replicaState[r][t1] \in {"committed", "executed", "applied"}
         /\ replicaState[r][t2] \in {"committed", "executed", "applied"}) =>
        replicaTS[r][t1] # replicaTS[r][t2]

DependencyConsistency ==
    \A r \in Replicas : \A t \in Transactions :
        (replicaState[r][t] \in {"committed", "executed", "applied"}) =>
        \A dep \in replicaDeps[r][t] :
            \/ ~ConflictingTxns(t, dep)
            \/ CompareTimestamp(replicaTS[r][dep], replicaTS[r][t])


CompleteDependencies ==
    \A r \in Replicas : \A t1, t2 \in Transactions :
        (/\ replicaState[r][t1] = "committed"
         /\ replicaState[r][t2] = "committed"
         /\ ConflictingTxns(t1, t2)
         /\ CompareTimestamp(replicaTS[r][t2], replicaTS[r][t1])) =>
        t2 \in replicaDeps[r][t1]


EventuallyAllCommitted ==
    <>[](\A t \in Transactions : TxnCommitted(t))

EventuallyAllApplied ==
    <>[](\A t \in Transactions : TxnApplied(t))


Liveness ==
    \A t \in Transactions :
        (TxnCommitted(t) ~> TxnApplied(t))

VARIABLES currentTime, currentRecBallot

vars == << replicaState, replicaTS, replicaDeps, replicaBallot, 
           replicaAcceptBallot, maxTSForKey, replicaEpoch, replicaResult, 
           replicaReads, coordState, coordTS, coordDeps, coordBallot, 
           coordResponses, coordReads, coordResult, recState, recBallot, 
           recResponses, recTS, recDeps, messages, txnKeys, txnCoord, txnOps, 
           currentTime, currentRecBallot >>

ProcSet == (Coordinators) \cup (Replicas) \cup ({"R1", "R2"})

Init == (* Global variables *)
        /\ replicaState = [r \in Replicas |-> [t \in Transactions |-> "init"]]
        /\ replicaTS =         [r \in Replicas |-> [t \in Transactions |->
                       CreateTimestamp(0, 0, 0, r)]]
        /\ replicaDeps = [r \in Replicas |-> [t \in Transactions |-> {}]]
        /\ replicaBallot = [r \in Replicas |-> [t \in Transactions |-> 0]]
        /\ replicaAcceptBallot = [r \in Replicas |-> [t \in Transactions |-> -1]]
        /\ maxTSForKey =           [r \in Replicas |-> [k \in Keys |->
                         CreateTimestamp(0, 0, 0, r)]]
        /\ replicaEpoch = [r \in Replicas |-> 0]
        /\ replicaResult = [r \in Replicas |-> [t \in Transactions |-> "none"]]
        /\ replicaReads = [r \in Replicas |-> [t \in Transactions |-> {}]]
        /\ coordState = [c \in Coordinators |-> [t \in Transactions |-> "init"]]
        /\ coordTS =       [c \in Coordinators |-> [t \in Transactions |->
                     CreateTimestamp(0, 0, 0, c)]]
        /\ coordDeps = [c \in Coordinators |-> [t \in Transactions |-> {}]]
        /\ coordBallot = [c \in Coordinators |-> [t \in Transactions |-> 0]]
        /\ coordResponses = [c \in Coordinators |-> [t \in Transactions |-> {}]]
        /\ coordReads = [c \in Coordinators |-> [t \in Transactions |-> {}]]
        /\ coordResult = [c \in Coordinators |-> [t \in Transactions |-> "none"]]
        /\ recState = [r \in {"R1", "R2"} |-> [t \in Transactions |-> "init"]]
        /\ recBallot = [r \in {"R1", "R2"} |-> [t \in Transactions |-> 0]]
        /\ recResponses = [r \in {"R1", "R2"} |-> [t \in Transactions |-> {}]]
        /\ recTS =     [r \in {"R1", "R2"} |-> [t \in Transactions |->
                   CreateTimestamp(0, 0, 0, r)]]
        /\ recDeps = [r \in {"R1", "R2"} |-> [t \in Transactions |-> {}]]
        /\ messages = {}
        /\ txnKeys = [t1 |-> {"k1"}, t2 |-> {"k1", "k2"}]
        /\ txnCoord = [t1 |-> "c1", t2 |-> "c1"]
        /\ txnOps = [t1 |-> <<"read", "k1">>, t2 |-> <<"write", "k1", "v1">>]
        (* Process coordinator *)
        /\ currentTime = [self \in Coordinators |-> 1]
        (* Process recovery *)
        /\ currentRecBallot = [self \in {"R1", "R2"} |-> MaxBallot \div 2]

coordinator(self) == /\ \/ /\ \E txn \in {t \in Transactions :
                                         /\ txnCoord[t] = self
                                         /\ coordState[self][t] = "init"}:
                                LET t0 == CreateTimestamp(0, currentTime[self], 0, self) IN
                                  /\ coordState' = [coordState EXCEPT ![self][txn] = "preaccept"]
                                  /\ coordTS' = [coordTS EXCEPT ![self][txn] = t0]
                                  /\ coordBallot' = [coordBallot EXCEPT ![self][txn] = 0]
                                  /\ coordResponses' = [coordResponses EXCEPT ![self][txn] = {}]
                                  /\ coordDeps' = [coordDeps EXCEPT ![self][txn] = {}]
                                  /\ currentTime' = [currentTime EXCEPT ![self] = currentTime[self] + 1]
                                  /\ messages' = (            messages \union {[
                                                      type |-> "PreAccept",
                                                      txn |-> txn,
                                                      coord |-> self,
                                                      ballot |-> 0,
                                                      timestamp |-> t0,
                                                      keys |-> txnKeys[txn],
                                                      dest |-> r
                                                  ] : r \in Replicas})
                           /\ UNCHANGED <<coordReads, coordResult>>
                        \/ /\ \E txn \in {t \in Transactions :
                                         /\ txnCoord[t] = self
                                         /\ coordState[self][t] = "preaccept"}:
                                /\   IsQuorum({r \in Replicas :
                                   \E msg \in messages :
                                       /\ msg.type = "PreAcceptOK"
                                       /\ msg.txn = txn
                                       /\ msg.coord = self
                                       /\ msg.ballot = coordBallot[self][txn]
                                       /\ msg.from = r})
                                /\ LET responses ==           {msg \in messages :
                                                    /\ msg.type = "PreAcceptOK"
                                                    /\ msg.txn = txn
                                                    /\ msg.coord = self
                                                    /\ msg.ballot = coordBallot[self][txn]} IN
                                     LET deps == UNION {msg.deps : msg \in responses} IN
                                       LET t0 == coordTS[self][txn] IN
                                         /\ coordResponses' = [coordResponses EXCEPT ![self][txn] = responses]
                                         /\ coordDeps' = [coordDeps EXCEPT ![self][txn] = deps]
                                         /\ IF IsFastQuorum({msg.from : msg \in responses}) /\
                                               \A msg \in responses : msg.timestamp = t0
                                               THEN /\ coordState' = [coordState EXCEPT ![self][txn] = "committed"]
                                                    /\ messages' = (            messages \union {[
                                                                        type |-> "Commit",
                                                                        txn |-> txn,
                                                                        coord |-> self,
                                                                        timestamp |-> t0,
                                                                        deps |-> deps,
                                                                        dest |-> r
                                                                    ] : r \in Replicas})
                                                    /\ UNCHANGED coordTS
                                               ELSE /\ LET decidedTS ==           CHOOSE ts \in {msg.timestamp : msg \in responses} :
                                                                        \A msg \in responses : ~CompareTimestamp(ts, msg.timestamp) IN
                                                         /\ coordState' = [coordState EXCEPT ![self][txn] = "accept"]
                                                         /\ coordTS' = [coordTS EXCEPT ![self][txn] = decidedTS]
                                                         /\ messages' = (            messages \union {[
                                                                             type |-> "Accept",
                                                                             txn |-> txn,
                                                                             coord |-> self,
                                                                             ballot |-> coordBallot[self][txn],
                                                                             timestamp |-> decidedTS,
                                                                             deps |-> deps,
                                                                             dest |-> r
                                                                         ] : r \in Replicas})
                           /\ UNCHANGED <<coordBallot, coordReads, coordResult, currentTime>>
                        \/ /\ \E txn \in {t \in Transactions :
                                         /\ txnCoord[t] = self
                                         /\ coordState[self][t] = "accept"}:
                                /\   IsQuorum({r \in Replicas :
                                   \E msg \in messages :
                                       /\ msg.type = "AcceptOK"
                                       /\ msg.txn = txn
                                       /\ msg.coord = self
                                       /\ msg.ballot = coordBallot[self][txn]
                                       /\ msg.from = r})
                                /\ LET acceptResponses ==                 {msg \in messages :
                                                          /\ msg.type = "AcceptOK"
                                                          /\ msg.txn = txn
                                                          /\ msg.coord = self
                                                          /\ msg.ballot = coordBallot[self][txn]} IN
                                     LET newDeps == UNION {msg.deps : msg \in acceptResponses} IN
                                       /\ coordDeps' = [coordDeps EXCEPT ![self][txn] = coordDeps[self][txn] \union newDeps]
                                       /\ coordState' = [coordState EXCEPT ![self][txn] = "committed"]
                                       /\ messages' = (            messages \union {[
                                                           type |-> "Commit",
                                                           txn |-> txn,
                                                           coord |-> self,
                                                           timestamp |-> coordTS[self][txn],
                                                           deps |-> coordDeps'[self][txn],
                                                           dest |-> r
                                                       ] : r \in Replicas})
                           /\ UNCHANGED <<coordTS, coordBallot, coordResponses, coordReads, coordResult, currentTime>>
                        \/ /\ \E txn \in {t \in Transactions :
                                         /\ txnCoord[t] = self
                                         /\ coordState[self][t] = "committed"}:
                                /\ coordState' = [coordState EXCEPT ![self][txn] = "reading"]
                                /\ coordReads' = [coordReads EXCEPT ![self][txn] = {}]
                                /\ messages' = (            messages \union {[
                                                    type |-> "Read",
                                                    txn |-> txn,
                                                    coord |-> self,
                                                    timestamp |-> coordTS[self][txn],
                                                    deps |-> coordDeps[self][txn],
                                                    keys |-> txnKeys[txn],
                                                    dest |-> CHOOSE r \in Replicas : TRUE
                                                ]})
                           /\ UNCHANGED <<coordTS, coordDeps, coordBallot, coordResponses, coordResult, currentTime>>
                        \/ /\ \E txn \in {t \in Transactions :
                                         /\ txnCoord[t] = self
                                         /\ coordState[self][t] = "reading"}:
                                /\   \E msg \in messages :
                                   /\ msg.type = "ReadOK"
                                   /\ msg.txn = txn
                                   /\ msg.coord = self
                                /\ LET readResp ==          CHOOSE msg \in messages :
                                                   /\ msg.type = "ReadOK"
                                                   /\ msg.txn = txn
                                                   /\ msg.coord = self IN
                                     LET result == "executed_" \o txn IN
                                       /\ coordResult' = [coordResult EXCEPT ![self][txn] = result]
                                       /\ coordState' = [coordState EXCEPT ![self][txn] = "applying"]
                                       /\ messages' = (            messages \union {[
                                                           type |-> "Apply",
                                                           txn |-> txn,
                                                           coord |-> self,
                                                           timestamp |-> coordTS[self][txn],
                                                           deps |-> coordDeps[self][txn],
                                                           result |-> result,
                                                           dest |-> r
                                                       ] : r \in Replicas})
                           /\ UNCHANGED <<coordTS, coordDeps, coordBallot, coordResponses, coordReads, currentTime>>
                        \/ /\ \E txn \in {t \in Transactions :
                                         /\ txnCoord[t] = self
                                         /\ coordState[self][t] = "applying"}:
                                /\   IsQuorum({r \in Replicas :
                                   replicaState[r][txn] = "applied"})
                                /\ coordState' = [coordState EXCEPT ![self][txn] = "done"]
                           /\ UNCHANGED <<coordTS, coordDeps, coordBallot, coordResponses, coordReads, coordResult, messages, currentTime>>
                     /\ UNCHANGED << replicaState, replicaTS, replicaDeps, 
                                     replicaBallot, replicaAcceptBallot, 
                                     maxTSForKey, replicaEpoch, replicaResult, 
                                     replicaReads, recState, recBallot, 
                                     recResponses, recTS, recDeps, txnKeys, 
                                     txnCoord, txnOps, currentRecBallot >>

replica(self) == /\ \/ /\ \E msg \in           {m \in messages :
                                     /\ m.type = "PreAccept"
                                     /\ m.dest = self}:
                            IF msg.ballot >= replicaBallot[self][msg.txn]
                               THEN /\ LET conflictKeys == msg.keys IN
                                         LET maxConflictTS ==               CHOOSE ts \in
                                                              {maxTSForKey[self][k] : k \in conflictKeys} :
                                                              \A ts2 \in {maxTSForKey[self][k] : k \in conflictKeys} :
                                                                  ~CompareTimestamp(ts, ts2) IN
                                           LET proposedTS == IF ~CompareTimestamp(msg.timestamp, maxConflictTS)
                                                             THEN msg.timestamp
                                                             ELSE IncrementTimestamp(maxConflictTS, self) IN
                                             LET localDeps ==           {t \in Transactions :
                                                              /\ t # msg.txn
                                                              /\ ConflictingTxns(t, msg.txn)
                                                              /\ replicaState[self][t] \in {"preaccepted", "accepted", "committed", "executed", "applied"}
                                                              /\ CompareTimestamp(replicaTS[self][t], msg.timestamp)} IN
                                               /\ replicaBallot' = [replicaBallot EXCEPT ![self][msg.txn] = msg.ballot]
                                               /\ replicaState' = [replicaState EXCEPT ![self][msg.txn] = "preaccepted"]
                                               /\ replicaTS' = [replicaTS EXCEPT ![self][msg.txn] = proposedTS]
                                               /\ replicaDeps' = [replicaDeps EXCEPT ![self][msg.txn] = localDeps]
                                               /\ maxTSForKey' = [maxTSForKey EXCEPT ![self] =                  [k \in Keys |->
                                                                                               IF k \in conflictKeys
                                                                                               THEN MaxTimestamp(maxTSForKey[self][k], proposedTS)
                                                                                               ELSE maxTSForKey[self][k]]]
                                               /\ messages' = (            messages \union {[
                                                                   type |-> "PreAcceptOK",
                                                                   txn |-> msg.txn,
                                                                   coord |-> msg.coord,
                                                                   ballot |-> msg.ballot,
                                                                   from |-> self,
                                                                   timestamp |-> proposedTS,
                                                                   deps |-> localDeps
                                                               ]})
                               ELSE /\ TRUE
                                    /\ UNCHANGED << replicaState, replicaTS, 
                                                    replicaDeps, replicaBallot, 
                                                    maxTSForKey, messages >>
                       /\ UNCHANGED <<replicaAcceptBallot, replicaResult, replicaReads>>
                    \/ /\ \E msg \in           {m \in messages :
                                     /\ m.type = "Accept"
                                     /\ m.dest = self}:
                            IF msg.ballot >= replicaBallot[self][msg.txn] /\
                               replicaState[self][msg.txn] # "committed"
                               THEN /\ LET localDeps ==           {t \in Transactions :
                                                        /\ t # msg.txn
                                                        /\ ConflictingTxns(t, msg.txn)
                                                        /\ replicaState[self][t] \in {"preaccepted", "accepted", "committed", "executed", "applied"}
                                                        /\ CompareTimestamp(replicaTS[self][t], msg.timestamp)} IN
                                         /\ replicaBallot' = [replicaBallot EXCEPT ![self][msg.txn] = msg.ballot]
                                         /\ replicaAcceptBallot' = [replicaAcceptBallot EXCEPT ![self][msg.txn] = msg.ballot]
                                         /\ replicaState' = [replicaState EXCEPT ![self][msg.txn] = "accepted"]
                                         /\ replicaTS' = [replicaTS EXCEPT ![self][msg.txn] = msg.timestamp]
                                         /\ replicaDeps' = [replicaDeps EXCEPT ![self][msg.txn] = msg.deps]
                                         /\ messages' = (            messages \union {[
                                                             type |-> "AcceptOK",
                                                             txn |-> msg.txn,
                                                             coord |-> msg.coord,
                                                             ballot |-> msg.ballot,
                                                             from |-> self,
                                                             deps |-> localDeps
                                                         ]})
                               ELSE /\ TRUE
                                    /\ UNCHANGED << replicaState, replicaTS, 
                                                    replicaDeps, replicaBallot, 
                                                    replicaAcceptBallot, 
                                                    messages >>
                       /\ UNCHANGED <<maxTSForKey, replicaResult, replicaReads>>
                    \/ /\ \E msg \in           {m \in messages :
                                     /\ m.type = "Commit"
                                     /\ m.dest = self}:
                            /\ replicaState' = [replicaState EXCEPT ![self][msg.txn] = "committed"]
                            /\ replicaTS' = [replicaTS EXCEPT ![self][msg.txn] = msg.timestamp]
                            /\ replicaDeps' = [replicaDeps EXCEPT ![self][msg.txn] = msg.deps]
                       /\ UNCHANGED <<replicaBallot, replicaAcceptBallot, maxTSForKey, replicaResult, replicaReads, messages>>
                    \/ /\ \E msg \in           {m \in messages :
                                     /\ m.type = "Read"
                                     /\ m.dest = self}:
                            IF AllDepsCommitted(msg.deps) /\
                               AllLowerDepsApplied(msg.deps, msg.timestamp, self)
                               THEN /\ replicaReads' = [replicaReads EXCEPT ![self][msg.txn] = txnKeys[msg.txn]]
                                    /\ messages' = (            messages \union {[
                                                        type |-> "ReadOK",
                                                        txn |-> msg.txn,
                                                        coord |-> msg.coord,
                                                        from |-> self,
                                                        reads |-> replicaReads'[self][msg.txn]
                                                    ]})
                               ELSE /\ TRUE
                                    /\ UNCHANGED << replicaReads, messages >>
                       /\ UNCHANGED <<replicaState, replicaTS, replicaDeps, replicaBallot, replicaAcceptBallot, maxTSForKey, replicaResult>>
                    \/ /\ \E msg \in           {m \in messages :
                                     /\ m.type = "Apply"
                                     /\ m.dest = self}:
                            IF AllDepsCommitted(msg.deps) /\
                               AllLowerDepsApplied(msg.deps, msg.timestamp, self)
                               THEN /\ replicaState' = [replicaState EXCEPT ![self][msg.txn] = "applied"]
                                    /\ replicaResult' = [replicaResult EXCEPT ![self][msg.txn] = msg.result]
                               ELSE /\ TRUE
                                    /\ UNCHANGED << replicaState, 
                                                    replicaResult >>
                       /\ UNCHANGED <<replicaTS, replicaDeps, replicaBallot, replicaAcceptBallot, maxTSForKey, replicaReads, messages>>
                    \/ /\ \E msg \in           {m \in messages :
                                     /\ m.type = "Recover"
                                     /\ m.dest = self}:
                            IF msg.ballot > replicaBallot[self][msg.txn]
                               THEN /\ replicaBallot' = [replicaBallot EXCEPT ![self][msg.txn] = msg.ballot]
                                    /\ IF replicaState[self][msg.txn] = "init"
                                          THEN /\ LET conflictKeys == txnKeys[msg.txn] IN
                                                    LET maxConflictTS ==               CHOOSE ts \in
                                                                         {maxTSForKey[self][k] : k \in conflictKeys} :
                                                                         \A ts2 \in {maxTSForKey[self][k] : k \in conflictKeys} :
                                                                             ~CompareTimestamp(ts, ts2) IN
                                                      LET proposedTS == IF ~CompareTimestamp(msg.timestamp, maxConflictTS)
                                                                        THEN msg.timestamp
                                                                        ELSE IncrementTimestamp(maxConflictTS, self) IN
                                                        /\ replicaState' = [replicaState EXCEPT ![self][msg.txn] = "preaccepted"]
                                                        /\ replicaTS' = [replicaTS EXCEPT ![self][msg.txn] = proposedTS]
                                                        /\ replicaDeps' = [replicaDeps EXCEPT ![self][msg.txn] = {}]
                                          ELSE /\ TRUE
                                               /\ UNCHANGED << replicaState, 
                                                               replicaTS, 
                                                               replicaDeps >>
                                    /\ LET superseding ==             {t \in Transactions :
                                                          /\ ConflictingTxns(t, msg.txn)
                                                          /\ msg.txn \notin replicaDeps'[self][t]
                                                          /\ ((\/ replicaState'[self][t] = "accepted"
                                                               /\ CompareTimestamp(msg.timestamp, replicaTS'[self][t]))
                                                              \/ (replicaState'[self][t] = "committed"
                                                               /\ CompareTimestamp(msg.timestamp, replicaTS'[self][t])))} IN
                                         LET waiting ==         {t \in Transactions :
                                                        /\ ConflictingTxns(t, msg.txn)
                                                        /\ replicaState'[self][t] = "accepted"
                                                        /\ CompareTimestamp(replicaTS'[self][t], msg.timestamp)
                                                        /\ CompareTimestamp(msg.timestamp, replicaTS'[self][t])} IN
                                           messages' = (            messages \union {[
                                                            type |-> "RecoverOK",
                                                            txn |-> msg.txn,
                                                            coord |-> msg.coord,
                                                            ballot |-> msg.ballot,
                                                            from |-> self,
                                                            state |-> replicaState'[self][msg.txn],
                                                            timestamp |-> replicaTS'[self][msg.txn],
                                                            deps |-> replicaDeps'[self][msg.txn],
                                                            acceptBallot |-> replicaAcceptBallot[self][msg.txn],
                                                            superseding |-> superseding,
                                                            waiting |-> waiting
                                                        ]})
                               ELSE /\ TRUE
                                    /\ UNCHANGED << replicaState, replicaTS, 
                                                    replicaDeps, replicaBallot, 
                                                    messages >>
                       /\ UNCHANGED <<replicaAcceptBallot, maxTSForKey, replicaResult, replicaReads>>
                 /\ UNCHANGED << replicaEpoch, coordState, coordTS, coordDeps, 
                                 coordBallot, coordResponses, coordReads, 
                                 coordResult, recState, recBallot, 
                                 recResponses, recTS, recDeps, txnKeys, 
                                 txnCoord, txnOps, currentTime, 
                                 currentRecBallot >>

recovery(self) == /\ \/ /\ \E txn \in Transactions:
                             LET t0 == CHOOSE ts \in {CreateTimestamp(0, t, 0, "c1") : t \in 1..MaxTime} : TRUE IN
                               IF recState[self][txn] = "init" /\
                                  currentRecBallot[self] > 0 /\
                                  \A r \in Replicas : currentRecBallot[self] > replicaBallot[r][txn]
                                  THEN /\ recState' = [recState EXCEPT ![self][txn] = "recovering"]
                                       /\ recBallot' = [recBallot EXCEPT ![self][txn] = currentRecBallot[self]]
                                       /\ recResponses' = [recResponses EXCEPT ![self][txn] = {}]
                                       /\ currentRecBallot' = [currentRecBallot EXCEPT ![self] = currentRecBallot[self] + 1]
                                       /\ messages' = (            messages \union {[
                                                           type |-> "Recover",
                                                           txn |-> txn,
                                                           coord |-> self,
                                                           ballot |-> recBallot'[self][txn],
                                                           timestamp |-> t0,
                                                           dest |-> r
                                                       ] : r \in Replicas})
                                  ELSE /\ TRUE
                                       /\ UNCHANGED << recState, recBallot, 
                                                       recResponses, messages, 
                                                       currentRecBallot >>
                        /\ UNCHANGED <<recTS, recDeps>>
                     \/ /\ \E txn \in {t \in Transactions : recState[self][t] = "recovering"}:
                             /\   IsQuorum({r \in Replicas :
                                \E msg \in messages :
                                    /\ msg.type = "RecoverOK"
                                    /\ msg.txn = txn
                                    /\ msg.coord = self
                                    /\ msg.ballot = recBallot[self][txn]
                                    /\ msg.from = r})
                             /\ LET responses ==           {msg \in messages :
                                                 /\ msg.type = "RecoverOK"
                                                 /\ msg.txn = txn
                                                 /\ msg.coord = self
                                                 /\ msg.ballot = recBallot[self][txn]} IN
                                  /\ recResponses' = [recResponses EXCEPT ![self][txn] = responses]
                                  /\ IF \E msg \in responses : msg.state = "applied"
                                        THEN /\ recState' = [recState EXCEPT ![self][txn] = "done"]
                                             /\ UNCHANGED << recTS, recDeps, 
                                                             messages >>
                                        ELSE /\ IF \E msg \in responses : msg.state = "committed"
                                                   THEN /\ \E msg \in {m \in responses : m.state = "committed"}:
                                                             /\ recTS' = [recTS EXCEPT ![self][txn] = msg.timestamp]
                                                             /\ recDeps' = [recDeps EXCEPT ![self][txn] = msg.deps]
                                                             /\ recState' = [recState EXCEPT ![self][txn] = "committing"]
                                                             /\ messages' = (            messages \union {[
                                                                                 type |-> "Commit",
                                                                                 txn |-> txn,
                                                                                 coord |-> self,
                                                                                 timestamp |-> msg.timestamp,
                                                                                 deps |-> msg.deps,
                                                                                 dest |-> r
                                                                             ] : r \in Replicas})
                                                   ELSE /\ IF \E msg \in responses : msg.state = "accepted"
                                                              THEN /\ \E msg \in       CHOOSE m \in {msg \in responses : msg.state = "accepted"} :
                                                                                 \A m2 \in {msg \in responses : msg.state = "accepted"} :
                                                                                     m.acceptBallot >= m2.acceptBallot:
                                                                        /\ recTS' = [recTS EXCEPT ![self][txn] = msg.timestamp]
                                                                        /\ recDeps' = [recDeps EXCEPT ![self][txn] = msg.deps]
                                                                        /\ recState' = [recState EXCEPT ![self][txn] = "accepting"]
                                                                        /\ messages' = (            messages \union {[
                                                                                            type |-> "Accept",
                                                                                            txn |-> txn,
                                                                                            coord |-> self,
                                                                                            ballot |-> recBallot[self][txn],
                                                                                            timestamp |-> msg.timestamp,
                                                                                            deps |-> msg.deps,
                                                                                            dest |-> r
                                                                                        ] : r \in Replicas})
                                                              ELSE /\ LET fastPathVotes ==               Cardinality({msg \in responses :
                                                                                           msg.timestamp = recTS[self][txn]}) IN
                                                                        LET superseding == UNION {msg.superseding : msg \in responses} IN
                                                                          LET waiting == UNION {msg.waiting : msg \in responses} IN
                                                                            IF superseding # {} \/ waiting # {}
                                                                               THEN /\ LET highestTS ==           CHOOSE ts \in {msg.timestamp : msg \in responses} :
                                                                                                        \A msg \in responses : ~CompareTimestamp(ts, msg.timestamp) IN
                                                                                         LET allDeps == UNION {msg.deps : msg \in responses} IN
                                                                                           /\ recTS' = [recTS EXCEPT ![self][txn] = highestTS]
                                                                                           /\ recDeps' = [recDeps EXCEPT ![self][txn] = allDeps]
                                                                                           /\ recState' = [recState EXCEPT ![self][txn] = "accepting"]
                                                                                           /\ messages' = (            messages \union {[
                                                                                                               type |-> "Accept",
                                                                                                               txn |-> txn,
                                                                                                               coord |-> self,
                                                                                                               ballot |-> recBallot[self][txn],
                                                                                                               timestamp |-> highestTS,
                                                                                                               deps |-> allDeps,
                                                                                                               dest |-> r
                                                                                                           ] : r \in Replicas})
                                                                               ELSE /\ LET allDeps == UNION {msg.deps : msg \in responses} IN
                                                                                         /\ recTS' = [recTS EXCEPT ![self][txn] = CreateTimestamp(0, 1, 0, "c1")]
                                                                                         /\ recDeps' = [recDeps EXCEPT ![self][txn] = allDeps]
                                                                                         /\ recState' = [recState EXCEPT ![self][txn] = "accepting"]
                                                                                         /\ messages' = (            messages \union {[
                                                                                                             type |-> "Accept",
                                                                                                             txn |-> txn,
                                                                                                             coord |-> self,
                                                                                                             ballot |-> recBallot[self][txn],
                                                                                                             timestamp |-> recTS'[self][txn],
                                                                                                             deps |-> allDeps,
                                                                                                             dest |-> r
                                                                                                         ] : r \in Replicas})
                        /\ UNCHANGED <<recBallot, currentRecBallot>>
                     \/ /\ \E txn \in {t \in Transactions : recState[self][t] = "accepting"}:
                             /\   IsQuorum({r \in Replicas :
                                \E msg \in messages :
                                    /\ msg.type = "AcceptOK"
                                    /\ msg.txn = txn
                                    /\ msg.coord = self
                                    /\ msg.ballot = recBallot[self][txn]
                                    /\ msg.from = r})
                             /\ LET acceptResponses ==                 {msg \in messages :
                                                       /\ msg.type = "AcceptOK"
                                                       /\ msg.txn = txn
                                                       /\ msg.coord = self
                                                       /\ msg.ballot = recBallot[self][txn]} IN
                                  LET newDeps == UNION {msg.deps : msg \in acceptResponses} IN
                                    /\ recDeps' = [recDeps EXCEPT ![self][txn] = recDeps[self][txn] \union newDeps]
                                    /\ recState' = [recState EXCEPT ![self][txn] = "committing"]
                                    /\ messages' = (            messages \union {[
                                                        type |-> "Commit",
                                                        txn |-> txn,
                                                        coord |-> self,
                                                        timestamp |-> recTS[self][txn],
                                                        deps |-> recDeps'[self][txn],
                                                        dest |-> r
                                                    ] : r \in Replicas})
                        /\ UNCHANGED <<recBallot, recResponses, recTS, currentRecBallot>>
                     \/ /\ \E txn \in {t \in Transactions :
                                      recState[self][t] \in {"committing", "done"}}:
                             recState' = [recState EXCEPT ![self][txn] = "done"]
                        /\ UNCHANGED <<recBallot, recResponses, recTS, recDeps, messages, currentRecBallot>>
                  /\ UNCHANGED << replicaState, replicaTS, replicaDeps, 
                                  replicaBallot, replicaAcceptBallot, 
                                  maxTSForKey, replicaEpoch, replicaResult, 
                                  replicaReads, coordState, coordTS, coordDeps, 
                                  coordBallot, coordResponses, coordReads, 
                                  coordResult, txnKeys, txnCoord, txnOps, 
                                  currentTime >>

Next == (\E self \in Coordinators: coordinator(self))
           \/ (\E self \in Replicas: replica(self))
           \/ (\E self \in {"R1", "R2"}: recovery(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 

====================================================================
