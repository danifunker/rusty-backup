# Third-party notices

`fs-pc` is MIT licensed (see `LICENSE`). The NTFS formatter embeds four template blobs in `src/ntfs_template/`:

- `attrdef.bin` (`$AttrDef`) and `upcase.bin` (`$UpCase`) are Microsoft's standard system-file data, identical on every NTFS volume of the same version.
- `secure_sds.bin` (the `$Secure:$SDS` stream) and `root_secdesc.bin` (the root directory's security descriptor) were captured from the output of `mkntfs` (ntfs-3g). They are data, not code; regenerating them from a Windows-formatted volume, or synthesising them from the published security-descriptor format, is a planned follow-up (rusty-backup licence decision D4).

The NTFS driver code itself is a clean-room implementation: no `mkntfs` or ntfs-3g source was read or translated.
