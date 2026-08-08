# Groundhog correctness invariants

1. Every transaction interpretation reads only the block-start snapshot.
2. Candidate interpretation may run concurrently; reservation decisions are deterministic.
3. A transaction reservation is atomic across all of its typed modifications.
4. A failed reservation restores every object touched by that transaction.
5. Negative nonnegative-integer reservations are bounded by the block-start base independently of positive reservations; same-block credits do not fund same-block withdrawals, and materialized values never fall below zero or overflow int64.
6. Different byte-string values cannot coexist for the same object in one block.
7. Ordered-set duplicate hashes and capacity violations cannot coexist in one block.
8. Fixed-block conflicts return an error before any state delta is exposed.
9. Successful fixed-block execution emits exactly one receipt and one transaction delta for every block transaction.
10. State root and receipt root are independent of worker count.
11. Groundhog never invokes Serial, Aria, Block-STM, or another fallback executor.
12. Existing block producers and executors retain their prior plugin identifiers, configurations, and behavior.
