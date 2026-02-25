v1.4 (2026-02-25)
=================
* Reduce default noise in all-user mode
* Tune SQLite for fast folder-constrained searches
* Drop redundant FTS delete in removeMessages
* Only index relevant folders, not search folders for example (genericOnly)
* Protect from deeply nested HTML divs (e.g. spam often does this)
* Skip users with no maildir in all-user mode


v1.3 (2025-02-05)
=================
* Add trace logging for message updates


v1.2 (2024-12-19)
=================
* Stop considering inline attachments


v1.1 (2024-12-12)
=================
* Prevent indexing of users without mailbox
* Index HTML body
* Evaluate PR_MESSAGE_DELIVERY_TIME before PR_LAST_MODIFICATION_TIME


v1.0 (2023-12-31)
=================

Behavioral changes:

* Merge grommunio-index-run.sh function in grommunio-index itself;
  new -A command-line option
