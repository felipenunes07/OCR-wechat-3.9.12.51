import unittest

from wechat_receipt_daemon import (
    WeChatMessageRef,
    normalize_amount,
    normalize_client_label,
    parse_receipt_fields,
    should_ignore_sender,
)


class NormalizeAmountTests(unittest.TestCase):
    def test_brazilian_grouping_uses_thousands_separator(self) -> None:
        self.assertEqual(normalize_amount("30.000"), 30000.0)
        self.assertEqual(normalize_amount("2.525"), 2525.0)

    def test_decimal_values_keep_fraction(self) -> None:
        self.assertEqual(normalize_amount("2,5"), 2.5)
        self.assertEqual(normalize_amount("30.000,00"), 30000.0)


class ParseReceiptFieldsTests(unittest.TestCase):
    def test_ignores_year_token_that_looks_like_currency(self) -> None:
        text = "\n".join(
            [
                "Comprovantedetransferencia",
                "20MAR2026-09:30:50",
                "Valor",
                "R$650,00",
                "Tipodetransferencia",
                "Pix",
                "IDdatransacao",
                "E18236120202603201229s0972ec9cf7",
                "Destino",
                "Nome",
                "CLEENDELETRONICOS",
                "CNPJ",
                "61964978000168",
                "Instituicao",
                "BCOBRADESCOS.A.",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "20/03/2026")
        self.assertEqual(fields["txn_time"], "09:30")
        self.assertEqual(fields["amount"], 650.0)

    def test_prefers_grouped_brl_amount(self) -> None:
        text = "\n".join(
            [
                "Comprovante de Pix",
                "20/03/2026 as 11:20:00",
                "Valor do pagamento",
                "R$30.000",
                "Destino",
                "Nome",
                "CLEENDELETRONICOS",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["amount"], 30000.0)


class ClientLabelTests(unittest.TestCase):
    def test_extracts_numeric_identifier_from_group_labels(self) -> None:
        self.assertEqual(normalize_client_label("2026 PP\u7fa4"), ("6", None))
        self.assertEqual(normalize_client_label("2026 65-2\u7fa4"), ("652", None))
        self.assertEqual(normalize_client_label("2026 116A\u7fa4"), ("116A", None))
        self.assertEqual(normalize_client_label("2026 65\u7fa4"), ("65", None))
        self.assertEqual(normalize_client_label("2026 16Boleto"), ("16", None))
        self.assertEqual(normalize_client_label(f"2026{chr(0x2014) * 5}1\u7fa4no\u7fa4\U0001f4b0"), ("1", None))

    def test_ignores_purely_decorative_group_labels(self) -> None:
        strawberries = "2026" + ("\U0001f353" * 6)
        self.assertEqual(normalize_client_label(strawberries), (None, "IGNORED_CLIENT_LABEL_DECORATIVE"))


class SenderIgnoreTests(unittest.TestCase):
    def test_ignores_configured_sender_ids(self) -> None:
        msg_ref = WeChatMessageRef(
            msg_svr_id="1",
            talker="27837425841@chatroom",
            create_time=1.0,
            sender_user_name="wxid_wml3ftd6qpea12",
            sender_display="Arthur Shelby",
            image_rel_path=None,
            thumb_rel_path=None,
            image_abs_path=None,
            thumb_abs_path=None,
        )
        self.assertTrue(should_ignore_sender(msg_ref))

    def test_allows_other_senders(self) -> None:
        msg_ref = WeChatMessageRef(
            msg_svr_id="2",
            talker="27837425841@chatroom",
            create_time=1.0,
            sender_user_name="wxid_cliente_real",
            sender_display="Cliente Real",
            image_rel_path=None,
            thumb_rel_path=None,
            image_abs_path=None,
            thumb_abs_path=None,
        )
        self.assertFalse(should_ignore_sender(msg_ref))


if __name__ == "__main__":
    unittest.main()
