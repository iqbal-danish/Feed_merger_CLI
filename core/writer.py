"""Streaming XML element writer."""

from __future__ import annotations

from pathlib import Path
from types import TracebackType
from lxml import etree


class XMLStreamWriter:
    """Streams XML element entries directly to disk in an open container node block."""

    def __init__(self, output_path: Path, root_node_name: str = "source") -> None:
        self.output_path = output_path
        self.root_node_name = root_node_name
        self.file = None

    def __enter__(self) -> XMLStreamWriter:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(self.output_path, "wb")
        
        # Write XML declaration and opening root tag
        self.file.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
        self.file.write(f"<{self.root_node_name}>\n".encode("utf-8"))
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.file:
            try:
                # Write closing root tag and flush output
                self.file.write(f"</{self.root_node_name}>\n".encode("utf-8"))
            finally:
                self.file.close()

    def write_element(self, job_dict: dict[str, str]) -> None:
        """Serialize a flat dictionary into an XML <job> node and write to stream."""
        if not self.file:
            raise RuntimeError("Writer stream is not open. Use with block.")

        job_elem = etree.Element("job")
        for key, val in job_dict.items():
            child = etree.SubElement(job_elem, key)
            if val:
                # Use CDATA block if description or has HTML markup
                if key in ("description", "title") or "<" in val or "&" in val:
                    child.text = etree.CDATA(val)
                else:
                    child.text = val

        # Serialize and write with indentation
        xml_str = etree.tostring(job_elem, pretty_print=True, encoding="utf-8")
        self.file.write(b"  " + xml_str.replace(b"\n", b"\n  ").rstrip(b" ") + b"\n")
