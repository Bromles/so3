---------------------------- MODULE Accord ----------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC, Naturals

CONSTANTS 
    Replicas,           \* Set of replica IDs
    Transactions,       \* Set of transaction IDs  
    Keys,              \* Set of keys that transactions can access
    Quorum,            \* Quorum size (majority)
    FastQuorum,        \* Fast path quorum size
    Coordinators,      \* Set of coordinator IDs
    MaxBallot,         \* Maximum ballot number
    MaxEpoch           \* Maximum epoch number

ASSUME /\ Quorum = (Cardinality(Replicas) \div 2) + 1
       /\ FastQuorum >= Quorum
       /\ FastQuorum <= Cardinality(Replicas)

\* Message types
MessageType == {"PreAccept", "PreAcceptOK", "Accept", "AcceptOK", 
                "Commit", "Recover", "RecoverOK"}

\* Transaction states
TxnState == {"init", "preaccepted", "accepted", "committed", "applied"}

\* Helper function to convert node ID to integer for comparison
NodeToInt(node) ==
    CASE node = "r1" -> 1
      [] node = "r2" -> 2
      [] node = "r3" -> 3
      [] node = "c1" -> 10
      [] OTHER -> 99

\* Helper functions
CompareTimestamp(ts1, ts2) ==
    IF ts1.epoch /= ts2.epoch THEN ts1.epoch < ts2.epoch
    ELSE IF ts1.time /= ts2.time THEN ts1.time < ts2.time  
    ELSE IF ts1.seq /= ts2.seq THEN ts1.seq < ts2.seq
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
    replicaState = [r \in Replicas |-> [t \in Transactions |-> "init"]];
    replicaTS = [r \in Replicas |-> [t \in Transactions |-> 
        CreateTimestamp(0, 0, 0, r)]];
    replicaDeps = [r \in Replicas |-> [t \in Transactions |-> {}]];
    replicaBallot = [r \in Replicas |-> [t \in Transactions |-> 0]];
    maxTSForKey = [r \in Replicas |-> [k \in Keys |-> 
        CreateTimestamp(0, 0, 0, r)]];
    replicaEpoch = [r \in Replicas |-> 0];
    
    coordState = [c \in Coordinators |-> [t \in Transactions |-> "init"]];
    coordTS = [c \in Coordinators |-> [t \in Transactions |-> 
        CreateTimestamp(0, 0, 0, c)]];
    coordDeps = [c \in Coordinators |-> [t \in Transactions |-> {}]];
    coordBallot = [c \in Coordinators |-> [t \in Transactions |-> 0]];
    coordResponses = [c \in Coordinators |-> [t \in Transactions |-> {}]];
    
    messages = {};
    txnKeys = [t1 |-> {"k1"}, t2 |-> {"k1", "k2"}];
    txnCoord = [t1 |-> "c1", t2 |-> "c1"];

define {
    ConflictingTxns(t1, t2) ==
        txnKeys[t1] \cap txnKeys[t2] /= {}

    \* Safety Properties
    ConsistentTimestamps == 
        \A r1, r2 \in Replicas : \A t \in Transactions :
            (/\ replicaState[r1][t] = "committed" 
             /\ replicaState[r2][t] = "committed") =>
            replicaTS[r1][t] = replicaTS[r2][t]

    UniqueTimestamps ==
        \A r \in Replicas : \A t1, t2 \in Transactions :
            (/\ t1 /= t2 
             /\ ConflictingTxns(t1, t2)
             /\ replicaState[r][t1] \in {"committed", "applied"}
             /\ replicaState[r][t2] \in {"committed", "applied"}) =>
            replicaTS[r][t1] /= replicaTS[r][t2]

    DependencyConsistency ==
        \A r \in Replicas : \A t \in Transactions :
            (replicaState[r][t] = "committed") =>
            \A dep \in replicaDeps[r][t] :
                \/ ~ConflictingTxns(t, dep)
                \/ CompareTimestamp(replicaTS[r][dep], replicaTS[r][t])
}

\* Coordinator process for transaction t
process (coordinator \in Coordinators)
variables
    txn = "t1";  \* Will be set in CoordStart
    phase = "preaccept";
    t0 = CreateTimestamp(0, 0, 0, self);
    decidedTS = t0;
    decidedDeps = {};
    currentBallot = 0;
{
CoordStart:
    \* Process each transaction that belongs to this coordinator
    with (t \in {tx \in Transactions : txnCoord[tx] = self}) {
        txn := t;
        t0 := CreateTimestamp(0, 1, 0, self);  \* Use a fixed time value
        goto PreAcceptPhase;
    };

PreAcceptPhase:
    \* Send PreAccept to all replicas
    coordState[self][txn] := "preaccept";
    coordTS[self][txn] := t0;
    coordBallot[self][txn] := currentBallot;
    coordResponses[self][txn] := {};
    
    messages := messages \union {[
        type |-> "PreAccept",
        txn |-> txn,
        coord |-> self,
        ballot |-> currentBallot,
        timestamp |-> t0,
        keys |-> txnKeys[txn],
        dest |-> r
    ] : r \in Replicas};
    
    goto WaitPreAcceptResponses;

WaitPreAcceptResponses:
    \* Wait for quorum of PreAcceptOK responses
    await IsQuorum({r \in Replicas : 
        \E msg \in messages : 
            /\ msg.type = "PreAcceptOK"
            /\ msg.txn = txn
            /\ msg.coord = self
            /\ msg.ballot = currentBallot
            /\ msg.from = r});
    
    \* Collect responses
    coordResponses[self][txn] := {msg \in messages : 
        /\ msg.type = "PreAcceptOK"
        /\ msg.txn = txn
        /\ msg.coord = self
        /\ msg.ballot = currentBallot};
    
    \* Collect all dependencies
    decidedDeps := UNION {msg.deps : msg \in coordResponses[self][txn]};
    coordDeps[self][txn] := decidedDeps;
    
    \* Check if fast path succeeded (all agree on t0)
    if (IsFastQuorum({msg.from : msg \in coordResponses[self][txn]}) /\
        \A msg \in coordResponses[self][txn] : msg.timestamp = t0) {
        \* Fast path success
        decidedTS := t0;
        goto CommitPhase;
    } else {
        \* Need slow path - pick highest timestamp
        decidedTS := CHOOSE ts \in {msg.timestamp : msg \in coordResponses[self][txn]} :
            \A msg \in coordResponses[self][txn] : 
                ~CompareTimestamp(ts, msg.timestamp);
        goto AcceptPhase;
    };

AcceptPhase:
    \* Send Accept with the decided timestamp
    coordState[self][txn] := "accept";
    coordTS[self][txn] := decidedTS;
    
    messages := messages \union {[
        type |-> "Accept",
        txn |-> txn,
        coord |-> self,
        ballot |-> currentBallot,
        timestamp |-> decidedTS,
        deps |-> decidedDeps,
        dest |-> r
    ] : r \in Replicas};
    
    goto WaitAcceptResponses;

WaitAcceptResponses:
    \* Wait for quorum of AcceptOK responses
    await IsQuorum({r \in Replicas :
        \E msg \in messages :
            /\ msg.type = "AcceptOK"
            /\ msg.txn = txn
            /\ msg.coord = self
            /\ msg.ballot = currentBallot
            /\ msg.from = r});
    
    \* Update dependencies with any new ones from Accept phase
    with (acceptResponses = {msg \in messages :
            /\ msg.type = "AcceptOK"
            /\ msg.txn = txn
            /\ msg.coord = self
            /\ msg.ballot = currentBallot}) {
        decidedDeps := decidedDeps \union 
            (UNION {msg.deps : msg \in acceptResponses});
        coordDeps[self][txn] := decidedDeps;
    };
    
    goto CommitPhase;

CommitPhase:
    \* Send Commit to all replicas
    coordState[self][txn] := "committed";
    
    messages := messages \union {[
        type |-> "Commit",
        txn |-> txn,
        coord |-> self,
        timestamp |-> decidedTS,
        deps |-> decidedDeps,
        dest |-> r
    ] : r \in Replicas};
    
    goto CoordFinish;

CoordFinish:
    skip;
}

\* Replica process
process (replica \in Replicas)
variables
    currentTxn = "t1";
    proposedTS = CreateTimestamp(0, 0, 0, self);
    conflictKeys = {};
    maxConflictTS = CreateTimestamp(0, 0, 0, self);
    localDeps = {};
{
ReplicaLoop:
    while (TRUE) {
        either {
            \* Handle PreAccept message
            with (msg \in {m \in messages : 
                /\ m.type = "PreAccept" 
                /\ m.dest = self}) {
                
                currentTxn := msg.txn;
                conflictKeys := msg.keys;
                
                \* Check ballot
                if (msg.ballot >= replicaBallot[self][currentTxn]) {
                    replicaBallot[self][currentTxn] := msg.ballot;
                    
                    \* Find max timestamp among conflicting keys
                    maxConflictTS := CHOOSE ts \in 
                        {maxTSForKey[self][k] : k \in conflictKeys} :
                        \A ts2 \in {maxTSForKey[self][k] : k \in conflictKeys} :
                            ~CompareTimestamp(ts, ts2);
                    
                    \* Propose timestamp
                    if (~CompareTimestamp(msg.timestamp, maxConflictTS)) {
                        proposedTS := msg.timestamp;
                    } else {
                        proposedTS := IncrementTimestamp(maxConflictTS, self);
                    };
                    
                    \* Update state
                    replicaState[self][currentTxn] := "preaccepted";
                    replicaTS[self][currentTxn] := proposedTS;
                    
                    \* Update max timestamp for keys
                    maxTSForKey[self] := [k \in Keys |->
                        IF k \in conflictKeys 
                        THEN MaxTimestamp(maxTSForKey[self][k], proposedTS)
                        ELSE maxTSForKey[self][k]];
                    
                    \* Find dependencies (conflicting txns with lower t0)
                    localDeps := {t \in Transactions :
                        /\ t /= currentTxn
                        /\ ConflictingTxns(t, currentTxn)
                        /\ replicaState[self][t] \in {"preaccepted", "accepted", "committed"}
                        /\ CompareTimestamp(coordTS[txnCoord[t]][t], msg.timestamp)};
                    
                    replicaDeps[self][currentTxn] := localDeps;
                    
                    \* Send response
                    messages := messages \union {[
                        type |-> "PreAcceptOK",
                        txn |-> currentTxn,
                        coord |-> msg.coord,
                        ballot |-> msg.ballot,
                        from |-> self,
                        timestamp |-> proposedTS,
                        deps |-> localDeps
                    ]};
                };
            }
        } or {
            \* Handle Accept message
            with (msg \in {m \in messages :
                /\ m.type = "Accept"
                /\ m.dest = self}) {
                
                currentTxn := msg.txn;
                
                \* Check ballot
                if (msg.ballot >= replicaBallot[self][currentTxn] /\
                    replicaState[self][currentTxn] /= "committed") {
                    
                    replicaBallot[self][currentTxn] := msg.ballot;
                    replicaState[self][currentTxn] := "accepted";
                    replicaTS[self][currentTxn] := msg.timestamp;
                    replicaDeps[self][currentTxn] := msg.deps;
                    
                    \* Find new dependencies based on accepted timestamp
                    localDeps := {t \in Transactions :
                        /\ t /= currentTxn
                        /\ ConflictingTxns(t, currentTxn)
                        /\ replicaState[self][t] \in {"preaccepted", "accepted", "committed"}
                        /\ CompareTimestamp(coordTS[txnCoord[t]][t], msg.timestamp)};
                    
                    \* Send response
                    messages := messages \union {[
                        type |-> "AcceptOK",
                        txn |-> currentTxn,
                        coord |-> msg.coord,
                        ballot |-> msg.ballot,
                        from |-> self,
                        deps |-> localDeps
                    ]};
                };
            }
        } or {
            \* Handle Commit message
            with (msg \in {m \in messages :
                /\ m.type = "Commit"
                /\ m.dest = self}) {
                
                currentTxn := msg.txn;
                
                \* Commit the transaction
                replicaState[self][currentTxn] := "committed";
                replicaTS[self][currentTxn] := msg.timestamp;
                replicaDeps[self][currentTxn] := msg.deps;
            }
        }
    }
}

}
*)
\* BEGIN TRANSLATION (chksum(pcal) = "a0b5fd6e" /\ chksum(tla) = "cd08f0fe")
VARIABLES replicaState, replicaTS, replicaDeps, replicaBallot, maxTSForKey, 
          replicaEpoch, coordState, coordTS, coordDeps, coordBallot, 
          coordResponses, messages, txnKeys, txnCoord, pc

(* define statement *)
ConflictingTxns(t1, t2) ==
    txnKeys[t1] \cap txnKeys[t2] /= {}


ConsistentTimestamps ==
    \A r1, r2 \in Replicas : \A t \in Transactions :
        (/\ replicaState[r1][t] = "committed"
         /\ replicaState[r2][t] = "committed") =>
        replicaTS[r1][t] = replicaTS[r2][t]

UniqueTimestamps ==
    \A r \in Replicas : \A t1, t2 \in Transactions :
        (/\ t1 /= t2
         /\ ConflictingTxns(t1, t2)
         /\ replicaState[r][t1] \in {"committed", "applied"}
         /\ replicaState[r][t2] \in {"committed", "applied"}) =>
        replicaTS[r][t1] /= replicaTS[r][t2]

DependencyConsistency ==
    \A r \in Replicas : \A t \in Transactions :
        (replicaState[r][t] = "committed") =>
        \A dep \in replicaDeps[r][t] :
            \/ ~ConflictingTxns(t, dep)
            \/ CompareTimestamp(replicaTS[r][dep], replicaTS[r][t])

VARIABLES txn, phase, t0, decidedTS, decidedDeps, currentBallot, currentTxn, 
          proposedTS, conflictKeys, maxConflictTS, localDeps

vars == << replicaState, replicaTS, replicaDeps, replicaBallot, maxTSForKey, 
           replicaEpoch, coordState, coordTS, coordDeps, coordBallot, 
           coordResponses, messages, txnKeys, txnCoord, pc, txn, phase, t0, 
           decidedTS, decidedDeps, currentBallot, currentTxn, proposedTS, 
           conflictKeys, maxConflictTS, localDeps >>

ProcSet == (Coordinators) \cup (Replicas)

Init == (* Global variables *)
        /\ replicaState = [r \in Replicas |-> [t \in Transactions |-> "init"]]
        /\ replicaTS =         [r \in Replicas |-> [t \in Transactions |->
                       CreateTimestamp(0, 0, 0, r)]]
        /\ replicaDeps = [r \in Replicas |-> [t \in Transactions |-> {}]]
        /\ replicaBallot = [r \in Replicas |-> [t \in Transactions |-> 0]]
        /\ maxTSForKey =           [r \in Replicas |-> [k \in Keys |->
                         CreateTimestamp(0, 0, 0, r)]]
        /\ replicaEpoch = [r \in Replicas |-> 0]
        /\ coordState = [c \in Coordinators |-> [t \in Transactions |-> "init"]]
        /\ coordTS =       [c \in Coordinators |-> [t \in Transactions |->
                     CreateTimestamp(0, 0, 0, c)]]
        /\ coordDeps = [c \in Coordinators |-> [t \in Transactions |-> {}]]
        /\ coordBallot = [c \in Coordinators |-> [t \in Transactions |-> 0]]
        /\ coordResponses = [c \in Coordinators |-> [t \in Transactions |-> {}]]
        /\ messages = {}
        /\ txnKeys = [t1 |-> {"k1"}, t2 |-> {"k1", "k2"}]
        /\ txnCoord = [t1 |-> "c1", t2 |-> "c1"]
        (* Process coordinator *)
        /\ txn = [self \in Coordinators |-> "t1"]
        /\ phase = [self \in Coordinators |-> "preaccept"]
        /\ t0 = [self \in Coordinators |-> CreateTimestamp(0, 0, 0, self)]
        /\ decidedTS = [self \in Coordinators |-> t0[self]]
        /\ decidedDeps = [self \in Coordinators |-> {}]
        /\ currentBallot = [self \in Coordinators |-> 0]
        (* Process replica *)
        /\ currentTxn = [self \in Replicas |-> "t1"]
        /\ proposedTS = [self \in Replicas |-> CreateTimestamp(0, 0, 0, self)]
        /\ conflictKeys = [self \in Replicas |-> {}]
        /\ maxConflictTS = [self \in Replicas |-> CreateTimestamp(0, 0, 0, self)]
        /\ localDeps = [self \in Replicas |-> {}]
        /\ pc = [self \in ProcSet |-> CASE self \in Coordinators -> "CoordStart"
                                        [] self \in Replicas -> "ReplicaLoop"]

CoordStart(self) == /\ pc[self] = "CoordStart"
                    /\ \E t \in {tx \in Transactions : txnCoord[tx] = self}:
                         /\ txn' = [txn EXCEPT ![self] = t]
                         /\ t0' = [t0 EXCEPT ![self] = CreateTimestamp(0, 1, 0, self)]
                         /\ pc' = [pc EXCEPT ![self] = "PreAcceptPhase"]
                    /\ UNCHANGED << replicaState, replicaTS, replicaDeps, 
                                    replicaBallot, maxTSForKey, replicaEpoch, 
                                    coordState, coordTS, coordDeps, 
                                    coordBallot, coordResponses, messages, 
                                    txnKeys, txnCoord, phase, decidedTS, 
                                    decidedDeps, currentBallot, currentTxn, 
                                    proposedTS, conflictKeys, maxConflictTS, 
                                    localDeps >>

PreAcceptPhase(self) == /\ pc[self] = "PreAcceptPhase"
                        /\ coordState' = [coordState EXCEPT ![self][txn[self]] = "preaccept"]
                        /\ coordTS' = [coordTS EXCEPT ![self][txn[self]] = t0[self]]
                        /\ coordBallot' = [coordBallot EXCEPT ![self][txn[self]] = currentBallot[self]]
                        /\ coordResponses' = [coordResponses EXCEPT ![self][txn[self]] = {}]
                        /\ messages' = (            messages \union {[
                                            type |-> "PreAccept",
                                            txn |-> txn[self],
                                            coord |-> self,
                                            ballot |-> currentBallot[self],
                                            timestamp |-> t0[self],
                                            keys |-> txnKeys[txn[self]],
                                            dest |-> r
                                        ] : r \in Replicas})
                        /\ pc' = [pc EXCEPT ![self] = "WaitPreAcceptResponses"]
                        /\ UNCHANGED << replicaState, replicaTS, replicaDeps, 
                                        replicaBallot, maxTSForKey, 
                                        replicaEpoch, coordDeps, txnKeys, 
                                        txnCoord, txn, phase, t0, decidedTS, 
                                        decidedDeps, currentBallot, currentTxn, 
                                        proposedTS, conflictKeys, 
                                        maxConflictTS, localDeps >>

WaitPreAcceptResponses(self) == /\ pc[self] = "WaitPreAcceptResponses"
                                /\   IsQuorum({r \in Replicas :
                                   \E msg \in messages :
                                       /\ msg.type = "PreAcceptOK"
                                       /\ msg.txn = txn[self]
                                       /\ msg.coord = self
                                       /\ msg.ballot = currentBallot[self]
                                       /\ msg.from = r})
                                /\ coordResponses' = [coordResponses EXCEPT ![self][txn[self]] =                          {msg \in messages :
                                                                                                 /\ msg.type = "PreAcceptOK"
                                                                                                 /\ msg.txn = txn[self]
                                                                                                 /\ msg.coord = self
                                                                                                 /\ msg.ballot = currentBallot[self]}]
                                /\ decidedDeps' = [decidedDeps EXCEPT ![self] = UNION {msg.deps : msg \in coordResponses'[self][txn[self]]}]
                                /\ coordDeps' = [coordDeps EXCEPT ![self][txn[self]] = decidedDeps'[self]]
                                /\ IF IsFastQuorum({msg.from : msg \in coordResponses'[self][txn[self]]}) /\
                                      \A msg \in coordResponses'[self][txn[self]] : msg.timestamp = t0[self]
                                      THEN /\ decidedTS' = [decidedTS EXCEPT ![self] = t0[self]]
                                           /\ pc' = [pc EXCEPT ![self] = "CommitPhase"]
                                      ELSE /\ decidedTS' = [decidedTS EXCEPT ![self] =          CHOOSE ts \in {msg.timestamp : msg \in coordResponses'[self][txn[self]]} :
                                                                                       \A msg \in coordResponses'[self][txn[self]] :
                                                                                           ~CompareTimestamp(ts, msg.timestamp)]
                                           /\ pc' = [pc EXCEPT ![self] = "AcceptPhase"]
                                /\ UNCHANGED << replicaState, replicaTS, 
                                                replicaDeps, replicaBallot, 
                                                maxTSForKey, replicaEpoch, 
                                                coordState, coordTS, 
                                                coordBallot, messages, txnKeys, 
                                                txnCoord, txn, phase, t0, 
                                                currentBallot, currentTxn, 
                                                proposedTS, conflictKeys, 
                                                maxConflictTS, localDeps >>

AcceptPhase(self) == /\ pc[self] = "AcceptPhase"
                     /\ coordState' = [coordState EXCEPT ![self][txn[self]] = "accept"]
                     /\ coordTS' = [coordTS EXCEPT ![self][txn[self]] = decidedTS[self]]
                     /\ messages' = (            messages \union {[
                                         type |-> "Accept",
                                         txn |-> txn[self],
                                         coord |-> self,
                                         ballot |-> currentBallot[self],
                                         timestamp |-> decidedTS[self],
                                         deps |-> decidedDeps[self],
                                         dest |-> r
                                     ] : r \in Replicas})
                     /\ pc' = [pc EXCEPT ![self] = "WaitAcceptResponses"]
                     /\ UNCHANGED << replicaState, replicaTS, replicaDeps, 
                                     replicaBallot, maxTSForKey, replicaEpoch, 
                                     coordDeps, coordBallot, coordResponses, 
                                     txnKeys, txnCoord, txn, phase, t0, 
                                     decidedTS, decidedDeps, currentBallot, 
                                     currentTxn, proposedTS, conflictKeys, 
                                     maxConflictTS, localDeps >>

WaitAcceptResponses(self) == /\ pc[self] = "WaitAcceptResponses"
                             /\   IsQuorum({r \in Replicas :
                                \E msg \in messages :
                                    /\ msg.type = "AcceptOK"
                                    /\ msg.txn = txn[self]
                                    /\ msg.coord = self
                                    /\ msg.ballot = currentBallot[self]
                                    /\ msg.from = r})
                             /\ LET acceptResponses ==                 {msg \in messages :
                                                       /\ msg.type = "AcceptOK"
                                                       /\ msg.txn = txn[self]
                                                       /\ msg.coord = self
                                                       /\ msg.ballot = currentBallot[self]} IN
                                  /\ decidedDeps' = [decidedDeps EXCEPT ![self] =            decidedDeps[self] \union
                                                                                  (UNION {msg.deps : msg \in acceptResponses})]
                                  /\ coordDeps' = [coordDeps EXCEPT ![self][txn[self]] = decidedDeps'[self]]
                             /\ pc' = [pc EXCEPT ![self] = "CommitPhase"]
                             /\ UNCHANGED << replicaState, replicaTS, 
                                             replicaDeps, replicaBallot, 
                                             maxTSForKey, replicaEpoch, 
                                             coordState, coordTS, coordBallot, 
                                             coordResponses, messages, txnKeys, 
                                             txnCoord, txn, phase, t0, 
                                             decidedTS, currentBallot, 
                                             currentTxn, proposedTS, 
                                             conflictKeys, maxConflictTS, 
                                             localDeps >>

CommitPhase(self) == /\ pc[self] = "CommitPhase"
                     /\ coordState' = [coordState EXCEPT ![self][txn[self]] = "committed"]
                     /\ messages' = (            messages \union {[
                                         type |-> "Commit",
                                         txn |-> txn[self],
                                         coord |-> self,
                                         timestamp |-> decidedTS[self],
                                         deps |-> decidedDeps[self],
                                         dest |-> r
                                     ] : r \in Replicas})
                     /\ pc' = [pc EXCEPT ![self] = "CoordFinish"]
                     /\ UNCHANGED << replicaState, replicaTS, replicaDeps, 
                                     replicaBallot, maxTSForKey, replicaEpoch, 
                                     coordTS, coordDeps, coordBallot, 
                                     coordResponses, txnKeys, txnCoord, txn, 
                                     phase, t0, decidedTS, decidedDeps, 
                                     currentBallot, currentTxn, proposedTS, 
                                     conflictKeys, maxConflictTS, localDeps >>

CoordFinish(self) == /\ pc[self] = "CoordFinish"
                     /\ TRUE
                     /\ pc' = [pc EXCEPT ![self] = "Done"]
                     /\ UNCHANGED << replicaState, replicaTS, replicaDeps, 
                                     replicaBallot, maxTSForKey, replicaEpoch, 
                                     coordState, coordTS, coordDeps, 
                                     coordBallot, coordResponses, messages, 
                                     txnKeys, txnCoord, txn, phase, t0, 
                                     decidedTS, decidedDeps, currentBallot, 
                                     currentTxn, proposedTS, conflictKeys, 
                                     maxConflictTS, localDeps >>

coordinator(self) == CoordStart(self) \/ PreAcceptPhase(self)
                        \/ WaitPreAcceptResponses(self)
                        \/ AcceptPhase(self) \/ WaitAcceptResponses(self)
                        \/ CommitPhase(self) \/ CoordFinish(self)

ReplicaLoop(self) == /\ pc[self] = "ReplicaLoop"
                     /\ \/ /\ \E msg \in           {m \in messages :
                                         /\ m.type = "PreAccept"
                                         /\ m.dest = self}:
                                /\ currentTxn' = [currentTxn EXCEPT ![self] = msg.txn]
                                /\ conflictKeys' = [conflictKeys EXCEPT ![self] = msg.keys]
                                /\ IF msg.ballot >= replicaBallot[self][currentTxn'[self]]
                                      THEN /\ replicaBallot' = [replicaBallot EXCEPT ![self][currentTxn'[self]] = msg.ballot]
                                           /\ maxConflictTS' = [maxConflictTS EXCEPT ![self] =              CHOOSE ts \in
                                                                                               {maxTSForKey[self][k] : k \in conflictKeys'[self]} :
                                                                                               \A ts2 \in {maxTSForKey[self][k] : k \in conflictKeys'[self]} :
                                                                                                   ~CompareTimestamp(ts, ts2)]
                                           /\ IF ~CompareTimestamp(msg.timestamp, maxConflictTS'[self])
                                                 THEN /\ proposedTS' = [proposedTS EXCEPT ![self] = msg.timestamp]
                                                 ELSE /\ proposedTS' = [proposedTS EXCEPT ![self] = IncrementTimestamp(maxConflictTS'[self], self)]
                                           /\ replicaState' = [replicaState EXCEPT ![self][currentTxn'[self]] = "preaccepted"]
                                           /\ replicaTS' = [replicaTS EXCEPT ![self][currentTxn'[self]] = proposedTS'[self]]
                                           /\ maxTSForKey' = [maxTSForKey EXCEPT ![self] =                  [k \in Keys |->
                                                                                           IF k \in conflictKeys'[self]
                                                                                           THEN MaxTimestamp(maxTSForKey[self][k], proposedTS'[self])
                                                                                           ELSE maxTSForKey[self][k]]]
                                           /\ localDeps' = [localDeps EXCEPT ![self] =          {t \in Transactions :
                                                                                       /\ t /= currentTxn'[self]
                                                                                       /\ ConflictingTxns(t, currentTxn'[self])
                                                                                       /\ replicaState'[self][t] \in {"preaccepted", "accepted", "committed"}
                                                                                       /\ CompareTimestamp(coordTS[txnCoord[t]][t], msg.timestamp)}]
                                           /\ replicaDeps' = [replicaDeps EXCEPT ![self][currentTxn'[self]] = localDeps'[self]]
                                           /\ messages' = (            messages \union {[
                                                               type |-> "PreAcceptOK",
                                                               txn |-> currentTxn'[self],
                                                               coord |-> msg.coord,
                                                               ballot |-> msg.ballot,
                                                               from |-> self,
                                                               timestamp |-> proposedTS'[self],
                                                               deps |-> localDeps'[self]
                                                           ]})
                                      ELSE /\ TRUE
                                           /\ UNCHANGED << replicaState, 
                                                           replicaTS, 
                                                           replicaDeps, 
                                                           replicaBallot, 
                                                           maxTSForKey, 
                                                           messages, 
                                                           proposedTS, 
                                                           maxConflictTS, 
                                                           localDeps >>
                        \/ /\ \E msg \in           {m \in messages :
                                         /\ m.type = "Accept"
                                         /\ m.dest = self}:
                                /\ currentTxn' = [currentTxn EXCEPT ![self] = msg.txn]
                                /\ IF msg.ballot >= replicaBallot[self][currentTxn'[self]] /\
                                      replicaState[self][currentTxn'[self]] /= "committed"
                                      THEN /\ replicaBallot' = [replicaBallot EXCEPT ![self][currentTxn'[self]] = msg.ballot]
                                           /\ replicaState' = [replicaState EXCEPT ![self][currentTxn'[self]] = "accepted"]
                                           /\ replicaTS' = [replicaTS EXCEPT ![self][currentTxn'[self]] = msg.timestamp]
                                           /\ replicaDeps' = [replicaDeps EXCEPT ![self][currentTxn'[self]] = msg.deps]
                                           /\ localDeps' = [localDeps EXCEPT ![self] =          {t \in Transactions :
                                                                                       /\ t /= currentTxn'[self]
                                                                                       /\ ConflictingTxns(t, currentTxn'[self])
                                                                                       /\ replicaState'[self][t] \in {"preaccepted", "accepted", "committed"}
                                                                                       /\ CompareTimestamp(coordTS[txnCoord[t]][t], msg.timestamp)}]
                                           /\ messages' = (            messages \union {[
                                                               type |-> "AcceptOK",
                                                               txn |-> currentTxn'[self],
                                                               coord |-> msg.coord,
                                                               ballot |-> msg.ballot,
                                                               from |-> self,
                                                               deps |-> localDeps'[self]
                                                           ]})
                                      ELSE /\ TRUE
                                           /\ UNCHANGED << replicaState, 
                                                           replicaTS, 
                                                           replicaDeps, 
                                                           replicaBallot, 
                                                           messages, localDeps >>
                           /\ UNCHANGED <<maxTSForKey, proposedTS, conflictKeys, maxConflictTS>>
                        \/ /\ \E msg \in           {m \in messages :
                                         /\ m.type = "Commit"
                                         /\ m.dest = self}:
                                /\ currentTxn' = [currentTxn EXCEPT ![self] = msg.txn]
                                /\ replicaState' = [replicaState EXCEPT ![self][currentTxn'[self]] = "committed"]
                                /\ replicaTS' = [replicaTS EXCEPT ![self][currentTxn'[self]] = msg.timestamp]
                                /\ replicaDeps' = [replicaDeps EXCEPT ![self][currentTxn'[self]] = msg.deps]
                           /\ UNCHANGED <<replicaBallot, maxTSForKey, messages, proposedTS, conflictKeys, maxConflictTS, localDeps>>
                     /\ pc' = [pc EXCEPT ![self] = "ReplicaLoop"]
                     /\ UNCHANGED << replicaEpoch, coordState, coordTS, 
                                     coordDeps, coordBallot, coordResponses, 
                                     txnKeys, txnCoord, txn, phase, t0, 
                                     decidedTS, decidedDeps, currentBallot >>

replica(self) == ReplicaLoop(self)

Next == (\E self \in Coordinators: coordinator(self))
           \/ (\E self \in Replicas: replica(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 

\* The translation will generate the following automatically

\* After translation, add these invariants:
Invariant == 
    /\ ConsistentTimestamps
    /\ UniqueTimestamps
    /\ DependencyConsistency

====================================================================
