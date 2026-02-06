# TODO: GPT Partition Table Support for Clonezilla Images

Clonezilla also supports GPT-partitioned disks. Their format stores:
- `sda-gpt.gdisk` or `sda-gpt.sgdisk` — GPT partition data
- No `sda-pt.sf` sfdisk file (or it contains GPT label)

Current implementation only handles MBR (sda-pt.sf with "label: dos").
Future work: parse GPT partition tables from Clonezilla images.

Also TODO: LVM and RAID configurations from Clonezilla are not supported.
