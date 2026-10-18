# Cache test ownership

`den-cache` owns engine behavior. Service crates should test only their adapter choices (key shape, value wrapper,
class/TTL/rank selection, scheduling, and metrics), rather than repeating filesystem mechanics.

| Engine contract | Canonical `den-cache` regression |
| --- | --- |
| Framed format, restart compatibility, legacy reads, torn-file rejection | `a_written_entry_carries_its_length_and_survives_a_restart`, `a_file_in_the_older_unframed_format_still_loads`, `a_torn_file_is_never_served` |
| Stable hashed/readable names | `names_are_the_ones_the_services_already_wrote` |
| Volatile/short-lived values retire durable state | `short_lived_entries_stay_in_memory_and_retire_the_file`, `volatile_entries_never_reach_disk_and_supersede_a_durable_one` |
| Indexed reads avoid directory/file probes | `the_directory_is_read_once_and_the_index_answers_the_rest`, `a_file_that_has_gone_is_a_miss_and_the_index_forgets_it` |
| Expiry/temp cleanup and write-failure recovery | sweep tests under “the sweep”; `a_failed_write_turns_persistence_off_until_the_next_sweep` |
| Byte budget and rebuild-cost ordering | `over_budget_the_sweep_spends_the_cheap_entries_first`, `within_a_cost_the_oldest_goes_first` |
| Generation-safe rewrite/retire/evict ownership | `a_stale_sweep_verdict_cannot_unlink_a_new_generation` |
| Memory byte bound, clock expiry, reader ownership | tests under “memory” and `a_reader_outlives_the_entry_it_read` |

The Scout adapter now keeps only its persistence threshold/class choice, independent disk-budget mapping, and sweep
task lifecycle in `crates/scout/src/tests/diskcache.rs`. Subtitles keeps its namespace cost ordering, text wrapper,
memory-only class, and memory/disk budget mapping beside `crates/subtitles/src/cache.rs`; shared filesystem and memory
engine mechanics live only in `den-cache`.
