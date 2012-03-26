Lokad Synapse Prototype
=======================

This is a prototype (or sample) of a simple set of servers that know how
to replicate events stored in Lokad.CQRS tape stream format.

event-replicator tracks file tape stream for any changes (tracking is done
per secondary file that contains last version number completely wtittent to that file)
and replicates its contents to one or more event stores defined in init file.


event-store is a simple acceptor that listens on the appropriate socket and accepts 
any event pushes.

elements know how to recover from network failures. If you set up an empty event store to listen
to event replicator, it will deliver all messages from the start before proceeding with 
continuous replication.

Important Notes
---------------

* This is just a prototype
* Binaries included are x86 bit. This might not work out-of-box on pure x64 
environment (like Windows Azure).
* You need to have something that writes to an event stream in format of Lokad.CQRS (included) AND 
keeps ver file updated.
* All hail ZeroMQ for the connectivity.