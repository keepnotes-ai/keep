"""Tests for email extraction in FileDocumentProvider."""

import textwrap
from pathlib import Path

import pytest

from keep.providers.documents import FileDocumentProvider


@pytest.fixture
def provider():
    return FileDocumentProvider()


class TestDetectEmail:
    """Tests for _detect_email content sniffing."""

    def test_simple_email(self, provider):
        content = textwrap.dedent("""\
            From: alice@example.com
            To: bob@example.com
            Subject: Hello

            Body text here.
        """)
        assert provider._detect_email(content) is True

    def test_not_email(self, provider):
        content = "Just some plain text\nwith multiple lines\n\nand a blank line."
        assert provider._detect_email(content) is False

    def test_single_header_not_enough(self, provider):
        content = "From: alice@example.com\n\nBody"
        assert provider._detect_email(content) is False

    def test_no_blank_line(self, provider):
        content = "From: alice@example.com\nTo: bob@example.com\nSubject: Hi"
        assert provider._detect_email(content) is False

    def test_enron_format(self, provider):
        content = textwrap.dedent("""\
            Message-ID: <12345.JavaMail.evans@thyme>
            Date: Mon, 4 Dec 2000 14:47:00 -0800 (PST)
            From: mike.grigsby@enron.com
            To: phillip.allen@enron.com
            Subject: issues
            Mime-Version: 1.0
            Content-Type: text/plain; charset=us-ascii
            Content-Transfer-Encoding: 7bit

            What do you think about amazon gift certificates?
        """)
        assert provider._detect_email(content) is True

    def test_yaml_frontmatter_not_email(self, provider):
        content = "---\ntitle: My Doc\nauthor: Alice\n---\n\nSome content."
        assert provider._detect_email(content) is False


class TestExtractEmail:
    """Tests for _extract_email parsing."""

    def test_simple_email(self, tmp_path, provider):
        email_file = tmp_path / "test.eml"
        email_file.write_text(textwrap.dedent("""\
            From: alice@example.com
            To: bob@example.com
            Subject: Hello World
            Date: Mon, 4 Dec 2000 14:47:00 -0800 (PST)
            Message-ID: <12345@example.com>

            This is the body of the email.
        """))

        content, tags, attachments = provider._extract_email(email_file)

        assert "This is the body of the email." in content
        assert tags['from'] == 'alice@example.com'
        assert tags['to'] == 'bob@example.com'
        assert tags['subject'] == 'Hello World'
        assert tags['message-id'] == '<12345@example.com>'
        assert 'date' in tags
        assert attachments is None

    def test_multiple_recipients(self, tmp_path, provider):
        email_file = tmp_path / "test.eml"
        email_file.write_text(textwrap.dedent("""\
            From: alice@example.com
            To: bob@example.com, carol@example.com
            Subject: Group email

            Hello everyone.
        """))

        content, tags, _ = provider._extract_email(email_file)

        assert tags['from'] == 'alice@example.com'
        assert isinstance(tags['to'], list)
        assert 'bob@example.com' in tags['to']
        assert 'carol@example.com' in tags['to']

    def test_cc_header(self, tmp_path, provider):
        email_file = tmp_path / "test.eml"
        email_file.write_text(textwrap.dedent("""\
            From: alice@example.com
            To: bob@example.com
            Cc: carol@example.com, dave@example.com
            Subject: FYI

            Please see attached.
        """))

        content, tags, _ = provider._extract_email(email_file)

        assert tags['to'] == 'bob@example.com'
        assert isinstance(tags['cc'], list)
        assert 'carol@example.com' in tags['cc']
        assert 'dave@example.com' in tags['cc']

    def test_display_names_stripped(self, tmp_path, provider):
        email_file = tmp_path / "test.eml"
        email_file.write_text(textwrap.dedent("""\
            From: "Alice Smith" <alice@example.com>
            To: "Bob Jones" <bob@example.com>
            Subject: Test

            Body.
        """))

        content, tags, _ = provider._extract_email(email_file)

        assert tags['from'] == 'alice@example.com'
        assert tags['to'] == 'bob@example.com'

    def test_addresses_lowercased(self, tmp_path, provider):
        email_file = tmp_path / "test.eml"
        email_file.write_text(textwrap.dedent("""\
            From: Alice@EXAMPLE.COM
            To: Bob@Example.Com
            Subject: Case test

            Body.
        """))

        content, tags, _ = provider._extract_email(email_file)

        assert tags['from'] == 'alice@example.com'
        assert tags['to'] == 'bob@example.com'

    def test_date_parsed_to_iso(self, tmp_path, provider):
        email_file = tmp_path / "test.eml"
        email_file.write_text(textwrap.dedent("""\
            From: alice@example.com
            To: bob@example.com
            Date: Mon, 4 Dec 2000 14:47:00 -0800 (PST)
            Subject: Date test

            Body.
        """))

        content, tags, _ = provider._extract_email(email_file)

        # Should be ISO 8601 format
        assert '2000-12-04' in tags['date']

    def test_transport_headers_skipped(self, tmp_path, provider):
        email_file = tmp_path / "test.eml"
        email_file.write_text(textwrap.dedent("""\
            From: alice@example.com
            To: bob@example.com
            Subject: Test
            Mime-Version: 1.0
            Content-Type: text/plain; charset=us-ascii
            Content-Transfer-Encoding: 7bit

            Body.
        """))

        content, tags, _ = provider._extract_email(email_file)

        assert 'mime-version' not in tags
        assert 'content-type' not in tags
        assert 'content-transfer-encoding' not in tags

    def test_enron_sample(self, tmp_path, provider):
        """Test with the actual Enron email format."""
        email_file = tmp_path / "grigsby-m_all_documents_325"
        email_file.write_text(textwrap.dedent("""\
            Message-ID: <10246476.1072135163201.JavaMail.evans@thyme>
            Date: Mon, 4 Dec 2000 14:47:00 -0800 (PST)
            From: mike.grigsby@enron.com
            To: phillip.allen@enron.com
            Subject: issues
            Mime-Version: 1.0
            Content-Type: text/plain; charset=us-ascii
            Content-Transfer-Encoding: 7bit

            What do you thnk about amazon gift certificates of $30.00 to the group?
        """))

        content, tags, _ = provider._extract_email(email_file)

        assert 'amazon gift certificates' in content
        assert tags['from'] == 'mike.grigsby@enron.com'
        assert tags['to'] == 'phillip.allen@enron.com'
        assert tags['subject'] == 'issues'
        assert tags['message-id'] == '<10246476.1072135163201.JavaMail.evans@thyme>'


class TestFetchEmail:
    """Tests for email detection via the fetch() path."""

    def test_fetch_eml_extension(self, tmp_path, provider, monkeypatch):
        """Files with .eml extension are parsed as email."""
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        email_file = tmp_path / "test.eml"
        email_file.write_text(textwrap.dedent("""\
            From: alice@example.com
            To: bob@example.com
            Subject: Hello

            Body text.
        """))

        doc = provider.fetch(str(email_file))

        assert doc.content_type == "message/rfc822"
        assert doc.tags is not None
        assert doc.tags['from'] == 'alice@example.com'
        assert 'Body text.' in doc.content

    def test_fetch_extensionless_email(self, tmp_path, provider, monkeypatch):
        """Extensionless files with email headers are auto-detected."""
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        email_file = tmp_path / "email_no_ext"
        email_file.write_text(textwrap.dedent("""\
            From: alice@example.com
            To: bob@example.com
            Subject: No extension

            This should still be detected as email.
        """))

        doc = provider.fetch(str(email_file))

        assert doc.content_type == "message/rfc822"
        assert doc.tags is not None
        assert doc.tags['from'] == 'alice@example.com'

    def test_fetch_plain_text_not_email(self, tmp_path, provider, monkeypatch):
        """Regular text files are not misdetected as email."""
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        text_file = tmp_path / "notes.txt"
        text_file.write_text("Just some notes\nabout things.\n\nMore notes.")

        doc = provider.fetch(str(text_file))

        assert doc.content_type == "text/plain"
        assert doc.tags is None


class TestMultipartEmail:
    """Tests for multipart email with attachments."""

    def _make_multipart_email(self, tmp_path, attachments=None):
        """Helper to build a multipart email with attachments."""
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders

        msg = MIMEMultipart()
        msg['From'] = 'alice@example.com'
        msg['To'] = 'bob@example.com'
        msg['Subject'] = 'Email with attachments'
        msg['Date'] = 'Mon, 4 Dec 2000 14:47:00 -0800 (PST)'
        msg['Message-ID'] = '<test-multipart@example.com>'

        msg.attach(MIMEText('Please see the attached files.', 'plain'))

        for att in (attachments or []):
            part = MIMEBase(att['maintype'], att['subtype'])
            part.set_payload(att['data'])
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition', 'attachment',
                filename=att['filename'],
            )
            msg.attach(part)

        email_file = tmp_path / "multipart.eml"
        email_file.write_bytes(msg.as_bytes())
        return email_file

    def test_extract_with_text_attachment(self, tmp_path):
        provider = FileDocumentProvider()
        email_file = self._make_multipart_email(tmp_path, [{
            'maintype': 'text',
            'subtype': 'plain',
            'data': b'This is a text attachment.',
            'filename': 'notes.txt',
        }])

        content, tags, attachments = provider._extract_email(email_file)

        assert 'Please see the attached files.' in content
        assert tags['from'] == 'alice@example.com'
        assert attachments is not None
        assert len(attachments) == 1
        assert attachments[0]['filename'] == 'notes.txt'
        assert attachments[0]['content_type'] == 'text/plain'
        assert attachments[0]['size'] > 0
        # Verify temp file exists
        assert Path(attachments[0]['path']).exists()
        # Clean up
        import shutil
        shutil.rmtree(Path(attachments[0]['path']).parent)

    def test_extract_with_binary_attachment(self, tmp_path):
        provider = FileDocumentProvider()
        fake_pdf = b'%PDF-1.4 fake pdf content here'
        email_file = self._make_multipart_email(tmp_path, [{
            'maintype': 'application',
            'subtype': 'pdf',
            'data': fake_pdf,
            'filename': 'report.pdf',
        }])

        content, tags, attachments = provider._extract_email(email_file)

        assert attachments is not None
        assert len(attachments) == 1
        assert attachments[0]['filename'] == 'report.pdf'
        assert attachments[0]['content_type'] == 'application/pdf'
        assert attachments[0]['size'] == len(fake_pdf)
        # Verify content was written correctly
        assert Path(attachments[0]['path']).read_bytes() == fake_pdf
        import shutil
        shutil.rmtree(Path(attachments[0]['path']).parent)

    def test_extract_multiple_attachments(self, tmp_path):
        provider = FileDocumentProvider()
        email_file = self._make_multipart_email(tmp_path, [
            {
                'maintype': 'image',
                'subtype': 'jpeg',
                'data': b'\xff\xd8\xff\xe0' + b'\x00' * 100,
                'filename': 'photo.jpg',
            },
            {
                'maintype': 'application',
                'subtype': 'octet-stream',
                'data': b'\x00\x01\x02\x03',
                'filename': 'data.bin',
            },
        ])

        content, tags, attachments = provider._extract_email(email_file)

        assert attachments is not None
        assert len(attachments) == 2
        assert attachments[0]['filename'] == 'photo.jpg'
        assert attachments[1]['filename'] == 'data.bin'
        import shutil
        shutil.rmtree(Path(attachments[0]['path']).parent)

    def test_no_attachments_returns_none(self, tmp_path):
        provider = FileDocumentProvider()
        email_file = self._make_multipart_email(tmp_path)

        content, tags, attachments = provider._extract_email(email_file)

        assert attachments is None

    def test_fetch_exposes_attachments_in_metadata(self, tmp_path, monkeypatch):
        """Attachments are exposed via Document.metadata._attachments."""
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        provider = FileDocumentProvider()
        email_file = self._make_multipart_email(tmp_path, [{
            'maintype': 'text',
            'subtype': 'plain',
            'data': b'Attachment content.',
            'filename': 'readme.txt',
        }])

        doc = provider.fetch(str(email_file))

        assert doc.content_type == "message/rfc822"
        assert doc.metadata is not None
        assert '_attachments' in doc.metadata
        assert len(doc.metadata['_attachments']) == 1
        assert doc.metadata['_attachments'][0]['filename'] == 'readme.txt'
        # Clean up temp files
        import shutil
        shutil.rmtree(Path(doc.metadata['_attachments'][0]['path']).parent)
