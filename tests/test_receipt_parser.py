import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image

from wechat_receipt_daemon import (
    IGNORED_SESSION_ROLLOVER_STATE,
    SESSION_PENDING_OPEN_STATE,
    GoogleSheetsSink,
    StateDB,
    WeChatDBResolver,
    WeChatMessageRef,
    backfill_missing_receipt_fields,
    candidate_initial_delay_seconds,
    hold_retry_delay_seconds,
    is_candidate,
    is_stale_pdf_modify,
    looks_like_single_receipt,
    normalize_amount,
    normalize_client_label,
    parse_receipt_fields,
    prepare_image_for_ocr,
    reconcile_scan,
    round_amount_for_output,
    runtime_media_resolver,
    seed_ready_manual_session_placeholders,
    should_ignore_sender,
)


class NormalizeAmountTests(unittest.TestCase):
    def test_brazilian_grouping_uses_thousands_separator(self) -> None:
        self.assertEqual(normalize_amount("30.000"), 30000.0)
        self.assertEqual(normalize_amount("2.525"), 2525.0)
        self.assertEqual(normalize_amount("6.60102"), 6601.02)

    def test_decimal_values_keep_fraction(self) -> None:
        self.assertEqual(normalize_amount("2,5"), 2.5)
        self.assertEqual(normalize_amount("30.000,00"), 30000.0)

    def test_round_amount_for_output_uses_half_up_rule(self) -> None:
        self.assertEqual(round_amount_for_output(1.52), 2.0)
        self.assertEqual(round_amount_for_output(1.49), 1.0)
        self.assertEqual(round_amount_for_output(0.50), 1.0)


class HoldRetryDelayTests(unittest.TestCase):
    def test_caps_retry_window_for_fast_manual_rechecks(self) -> None:
        self.assertEqual(hold_retry_delay_seconds(100.0, 107.0), 5)
        self.assertEqual(hold_retry_delay_seconds(100.0, 103.0), 3)
        self.assertEqual(hold_retry_delay_seconds(100.0, 100.4), 2)


class CandidateInitialDelayTests(unittest.TestCase):
    def test_temp_preview_waits_a_bit_longer_when_direct_images_are_preferred(self) -> None:
        self.assertEqual(candidate_initial_delay_seconds("temp_image", 1, thumb_candidates_enabled=False), 3)
        self.assertEqual(candidate_initial_delay_seconds("msgattach_image_dat", 1, thumb_candidates_enabled=False), 1)
        self.assertEqual(candidate_initial_delay_seconds("temp_image", 5, thumb_candidates_enabled=False), 5)


class PrepareImageForOCRTests(unittest.TestCase):
    def test_downscales_large_non_thumb_images_to_1600_max_side(self) -> None:
        img = Image.new("RGB", (1242, 2208), "white")

        prepared = prepare_image_for_ocr(img, "msgattach_image_dat")

        self.assertEqual(prepared.size, (900, 1600))

    def test_keeps_normal_non_thumb_images_at_original_size(self) -> None:
        img = Image.new("RGB", (640, 1600), "white")

        prepared = prepare_image_for_ocr(img, "msgattach_image_dat")

        self.assertEqual(prepared.size, (640, 1600))


class RuntimeMediaResolverTests(unittest.TestCase):
    def test_returns_none_when_resolver_is_degraded(self) -> None:
        resolver = WeChatDBResolver.__new__(WeChatDBResolver)
        resolver._last_error = "pywxdump_unavailable"

        self.assertIsNone(runtime_media_resolver(resolver))

    def test_keeps_resolver_when_no_runtime_error(self) -> None:
        resolver = WeChatDBResolver.__new__(WeChatDBResolver)
        resolver._last_error = None

        self.assertIs(runtime_media_resolver(resolver), resolver)

    def test_merge_failure_keeps_resolver_while_stale_index_exists(self) -> None:
        # Real regression 29/07/2026: one WinError 32 during the index swap set
        # a sticky rename_failed error; gating the resolver off on it meant
        # refresh_if_due() was never called again, so the failure could never
        # clear and every PDF held forever. A stale index is still queryable.
        with tempfile.TemporaryDirectory() as tmp_dir:
            stale_index = Path(tmp_dir) / "wechat_merge.db"
            stale_index.write_bytes(b"")
            resolver = WeChatDBResolver.__new__(WeChatDBResolver)
            resolver.merge_path = stale_index
            resolver._last_error = (
                "rename_failed:PermissionError:[WinError 32] O arquivo ja esta "
                "sendo usado por outro processo"
            )
            self.assertIs(runtime_media_resolver(resolver), resolver)

            resolver._last_error = "merge_failed:timeout"
            self.assertIs(runtime_media_resolver(resolver), resolver)

    def test_merge_failure_without_index_on_disk_disables_resolver(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            resolver = WeChatDBResolver.__new__(WeChatDBResolver)
            resolver.merge_path = Path(tmp_dir) / "nunca_criado.db"
            resolver._last_error = "merge_failed:timeout"
            self.assertIsNone(runtime_media_resolver(resolver))


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
        self.assertEqual(fields["txn_date_source"], "parsed")
        self.assertEqual(fields["txn_time_source"], "parsed")

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
        self.assertEqual(fields["amount_rounded"], 30000.0)

    def test_parses_full_month_and_compact_cent_fix(self) -> None:
        text = "\n".join(
            [
                "Comprovante de Pix",
                "20/marco/2026 as 11h35.",
                "R$ 66804",
                "Banco Bradesco",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "20/03/2026")
        self.assertEqual(fields["txn_time"], "11:35")
        self.assertEqual(fields["amount"], 668.04)
        self.assertEqual(fields["amount_rounded"], 668.0)
        self.assertEqual(fields["amount_source"], "currency_compact_cent_fix")

    def test_mercado_pago_superscript_cent_splits_two_cent_digits(self) -> None:
        # Real OCR text: MP renders cents as small superscript digits that OCR glues
        # onto the value. Cents are always TWO digits on Brazilian receipts, so a
        # glued no-separator token splits its last two digits ("R$ 8374" -> 83,74).
        # Confirmed by real receipts of 23-24/07/2026: "6737" was R$ 67,37 and
        # "3720" was R$ 37,20 (the old one-digit split launched 673,7/372,0).
        text = "\n".join(
            [
                "mercado",
                "pago",
                "Comprovante de Pix",
                "22/julho/2026as15:55:44.",
                "R$ 8374",
                "De",
                "20.914.890CARISAGONCALVESDE",
                "Para",
                "Amd RepresentacoeseServicosLtda",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["amount"], 83.74)
        self.assertEqual(fields["amount_rounded"], 84.0)
        self.assertEqual(fields["amount_source"], "currency_superscript_cent_fix")

    def test_mp_thousands_with_glued_superscript_cent_digit(self) -> None:
        # Real receipt 24/07/2026 16:40: R$ 1.741,xx OCR'd as "R$ 1.7419".
        # Old behavior parsed the thousands dot as decimal and launched 1,74.
        text = "\n".join(
            [
                "mercado",
                "pago",
                "Comprovante de Pix",
                "24/julho/2026as16:40:05.",
                "R$ 1.7419",
                "Origemedestino",
                "DEIVIDWILLIANSOUZARAMOS",
                "Amd RepresentacoeseServicosLtda",
                "BANCODOBRASILS.A.",
            ]
        )
        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)
        self.assertEqual(fields["amount"], 1741.09)
        self.assertEqual(fields["amount_rounded"], 1741.0)

    def test_currency_with_ocr_dot_instead_of_decimal_comma(self) -> None:
        # Real receipt 24/07/2026 13:09 (Santander): "R$26.129,00" OCR'd as
        # "R$26.129.00". Old behavior failed to parse and launched an empty value.
        text = "\n".join(
            [
                "Santander",
                "Comprovantedopagamento",
                "24/07/2026-13:09:29",
                "Valordopagamento",
                "R$26.129.00",
                "Tipo detransferencia",
                "Pix",
            ]
        )
        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)
        self.assertEqual(fields["amount"], 26129.0)

    def test_masked_pix_key_never_wins_over_real_value(self) -> None:
        # Real receipt 24/07/2026 14:59 (REUNIBANK): value "R$1.695.00" plus a
        # masked Pix key "**9.762.666-**". Old behavior launched 9.762.666,00.
        text = "\n".join(
            [
                "REUNIBANK",
                "ComprovantedeTransferencia",
                "DadosdaTransacao",
                "ID da transacao:1766",
                "Valortotal:R$1.695.00",
                "Status: Sucesso",
                "Data:24/07/2026",
                "Horario:14:59",
                "Nome:CLEENDINTERMEDIACAOEATACADOLTDA",
                "Chave pix:**9.762.666-**",
            ]
        )
        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)
        self.assertEqual(fields["amount"], 1695.0)

    def test_year_glued_to_hour_never_beats_currency_value(self) -> None:
        # Real receipt 24/07/2026 18:10 (Caixa app): date line "24/07/2026,18:10:54"
        # produced fallback candidate "2026,18" that outranked "R$933,00" because
        # "Pix enviado" sat right above the date line. Old behavior launched 2026,18.
        text = "\n".join(
            [
                "Pix enviado",
                "24/07/2026,18:10:54",
                "R$933,00",
                "Valor",
                "Recebedor",
                "Cleend Eletronicos",
            ]
        )
        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)
        self.assertEqual(fields["amount"], 933.0)

    def test_caixa_ted_slip_is_a_receipt_and_uses_valor_not_total(self) -> None:
        # Real receipt 24/07/2026 15:19 (Caixa printed TED slip): was discarded as
        # TABULAR_TRANSFER_LIST (has data/hora/banco/transfer + TOTAL); and TOTAL
        # (value+fee) must not beat VALOR DA TED.
        text = "\n".join(
            [
                "CAIXA ECONOMICA FEDERAL",
                "DATA: 24/07/2026 HORA: 15:19:16",
                "TERMINAL:1102 NSU:000339",
                "RECIBO DE ENVIO DE TED - AGENCIA 3053",
                "REMETENTE:",
                "BANCO: CAIXA ECONOMICA FEDERAL AG: 3053-8",
                "NOME: WILBERT JORGE CCOYO",
                "DESTINATARIO:",
                "BCO DO BRASIL S.A.",
                "NOME: AMD REPRESENTACOES E SERVICOS LTDA",
                "VALOR DA TED : 10.000,00",
                "TARIFA SERVICO : 25,00",
                "TOTAL : 10.025,00",
                "AUTENTICACAO",
                "CEF30532407260003701000339 10.025,00RD1102",
                "DEBITO REALIZADO COM SUCESSO. A PREVISAO DE",
                "CREDITO NA CONTA DE DESTINO E DE 60 MINUTOS.",
            ]
        )
        is_receipt, reason = looks_like_single_receipt(text)
        self.assertTrue(is_receipt, reason)
        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)
        self.assertEqual(fields["amount"], 10000.0)

    def test_pdf_under_filestorage_file_is_candidate_and_detected(self) -> None:
        from wechat_receipt_daemon import detect_source_kind

        pdf = Path(r"C:\Users\x\Documents\WeChat Files\wxid_a\FileStorage\File\2026-07\comprovante.pdf")
        self.assertEqual(detect_source_kind(pdf), "file_pdf")
        xlsx = Path(r"C:\Users\x\Documents\WeChat Files\wxid_a\FileStorage\File\2026-07\planilha.xlsx")
        self.assertEqual(detect_source_kind(xlsx), "other")

    def test_photo_with_six_deposit_slips_splits_into_six_receipts(self) -> None:
        # Real scenario 26/07/2026: client photographs six Banco do Brasil deposit
        # slips in a 2-column x 3-row grid. Reading OCR output top-to-bottom
        # interleaves the columns; the spatial splitter must yield one receipt
        # per slip, each with its own value.
        from wechat_receipt_daemon import OCRSpan, split_receipt_segments

        def slip(x0: float, y0: float, hora: str, valor: str) -> list[OCRSpan]:
            def span(dx: float, dy: float, text: str, w: float = 300.0) -> OCRSpan:
                return OCRSpan(left=x0 + dx, top=y0 + dy, right=x0 + dx + w, bottom=y0 + dy + 18, text=text, conf=0.95)

            return [
                span(0, 0, f"22/07/2026 - BANCO DO BRASIL - {hora}"),
                span(0, 22, "COMPROVANTE DE DEPOSITO EM CONTA CORRENTE"),
                span(0, 44, "EM DINHEIRO"),
                span(0, 66, "CLIENTE: AMD R SERVICOS LTDA"),
                span(0, 88, "DATA :"),
                span(220, 88, "22/07/2026", w=110),
                span(0, 110, "VALOR DINHEIRO"),
                span(220, 110, valor, w=110),
                span(0, 132, "VALOR TOTAL"),
                span(220, 132, valor, w=110),
            ]

        spans: list[OCRSpan] = []
        valores = ["1.850,00", "1.900,00", "2.000,00", "2.000,00", "1.750,00", "50,00"]
        horas = ["09:51:18", "09:43:06", "09:46:42", "09:47:39", "09:48:58", "09:58:23"]
        i = 0
        for row in range(3):
            for col in range(2):
                spans.extend(slip(60 + col * 480, 40 + row * 260, horas[i], valores[i]))
                i += 1
        # Interleave like real OCR (sorted top-to-bottom) to prove order doesn't matter.
        spans.sort(key=lambda s: (s.top, s.left))

        segments = split_receipt_segments(spans)
        self.assertEqual(len(segments), 6)
        parsed = []
        for seg in segments:
            ok, reason = looks_like_single_receipt(seg)
            self.assertTrue(ok, reason)
            fields = parse_receipt_fields(seg, ocr_conf=0.95, q_score=0.9)
            parsed.append((fields["amount"], fields["txn_time"]))
        self.assertEqual(sorted(v for v, _ in parsed), [50.0, 1750.0, 1850.0, 1900.0, 2000.0, 2000.0])
        self.assertIn(("1850.0", "09:51")[0] and 1850.0, [v for v, _ in parsed])
        self.assertEqual(sorted(t for _, t in parsed), sorted(["09:51", "09:43", "09:46", "09:47", "09:48", "09:58"]))

    def test_pix_transaction_id_extracted_even_when_split_across_lines(self) -> None:
        from wechat_receipt_daemon import extract_pix_transaction_ids

        # Part 1 (with value): id broken across two lines by OCR.
        parte1 = "ID da transacao\nE182361202026072\n41838s0035c5cb09\nDestino"
        # Part 2 (sender print): same id in one line.
        parte2 = "ID da transacao:\nE18236120202607241838s0035c5cb09\nEstamos aqui para ajudar"
        ids1 = extract_pix_transaction_ids(parte1)
        ids2 = extract_pix_transaction_ids(parte2)
        self.assertTrue(ids1 & ids2)

    def test_sender_only_print_is_flagged_as_continuation(self) -> None:
        from wechat_receipt_daemon import looks_like_sender_continuation

        parte2 = "\n".join(
            [
                "Origem",
                "Nome Leandro Coelho dos Santos",
                "Instituicao NU PAGAMENTOS - IP",
                "CPF ***.086.327-**",
                "Informacoes adicionais",
                "Identificador AMD",
            ]
        )
        self.assertTrue(looks_like_sender_continuation(parte2))
        # A full receipt still shows the "Valor" label even when OCR misses digits.
        parte1 = "Comprovante de pagamento\n24 JUL 2026 - 15:38:18\nValor R$ 5.400,00\nOrigem"
        self.assertFalse(looks_like_sender_continuation(parte1))

    def test_promo_marker_does_not_match_inside_payer_names(self) -> None:
        # Real regression found 26/07/2026: "audio" matched inside "Claudio" and
        # "RECAUDIOVISUAL", discarding the true value of legitimate receipts.
        text = "\n".join(
            [
                "Comprovante de Pagamento Pix",
                "Sicredi",
                "Valor: RS 2.447,00",
                "Realizado em: 17/06/2026 - 14:04:01",
                "Solicitante: CLAUDIO NAPOLEAO PERTINHEZ",
            ]
        )
        fields = parse_receipt_fields(text, ocr_conf=0.95, q_score=0.9)
        self.assertEqual(fields["amount"], 2447.0)

        text2 = "\n".join(
            [
                "valordatransferencia",
                "R$10.000,00",
                "de",
                "RECAUDIOVISUALPRODUTORALTDA",
            ]
        )
        fields2 = parse_receipt_fields(text2, ocr_conf=0.95, q_score=0.9)
        self.assertEqual(fields2["amount"], 10000.0)

    def test_ocr_mangled_promo_banner_still_ignored(self) -> None:
        # Real regression risk 26/07/2026: OCR glues the banner words
        # ("mensagemouaudio", "Baixeoappeconheca"); two distinct markers in the
        # context, even glued, must still discard the banner value.
        text = "\n".join(
            [
                "ComprovantedePix",
                "3/julho/2026as20:59:39.",
                "R$ 165",
                "De",
                "PajntiSuya",
                "MercadoPago",
                "Assistente pessoal",
                "FacaPixepagamentospor",
                "RS 50 pers Ane 3",
                "Fega um Pix de",
                "mensagemouaudio",
                "Baixeoappeconheca!",
            ]
        )
        fields = parse_receipt_fields(text, ocr_conf=0.95, q_score=0.9)
        self.assertEqual(fields["amount"], 165.0)

    def test_promo_banner_with_standalone_marker_still_ignored(self) -> None:
        # The Mercado Pago assistant banner must still be discarded.
        text = "\n".join(
            [
                "Envie um audio para o assistente",
                "R$ 50 para Ana",
                "baixe o app",
                "Comprovante de Pix",
                "Valor",
                "R$ 730,00",
            ]
        )
        fields = parse_receipt_fields(text, ocr_conf=0.95, q_score=0.9)
        self.assertEqual(fields["amount"], 730.0)

    def test_row_color_priority(self) -> None:
        from wechat_receipt_daemon import (
            SHEET_CLEAR_COLOR,
            SHEET_GUESS_COLOR,
            SHEET_NO_BANK_COLOR,
            SHEET_PDF_COLOR,
            sheet_row_color,
        )

        self.assertEqual(sheet_row_color({"bank": "AMD", "amount": 10.0}), SHEET_CLEAR_COLOR)
        self.assertEqual(sheet_row_color({"bank": None, "amount": 10.0}), SHEET_NO_BANK_COLOR)
        self.assertEqual(sheet_row_color({"bank": "AMD", "amount": 10.0, "is_pdf": True}), SHEET_PDF_COLOR)
        # Unknown bank outranks the informational PDF blue.
        self.assertEqual(sheet_row_color({"bank": "", "amount": 10.0, "is_pdf": True}), SHEET_NO_BANK_COLOR)
        # Guessed value outranks everything.
        self.assertEqual(
            sheet_row_color({"bank": None, "value_uncertain": True, "is_pdf": True}), SHEET_GUESS_COLOR
        )

    def test_single_receipt_is_not_split(self) -> None:
        from wechat_receipt_daemon import OCRSpan, split_receipt_segments

        spans = [
            OCRSpan(10, 10, 300, 28, "Comprovante de Pix", 0.9),
            OCRSpan(10, 40, 200, 58, "R$ 933,00", 0.9),
            OCRSpan(10, 70, 200, 88, "Valor", 0.9),
        ]
        self.assertEqual(split_receipt_segments(spans), [])

    def test_non_mercado_pago_compact_cent_fix_still_splits_two_digits(self) -> None:
        text = "\n".join(
            [
                "Comprovante de Pix",
                "20/marco/2026 as 11h35.",
                "R$ 8374",
                "Banco Bradesco",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["amount"], 83.74)
        self.assertEqual(fields["amount_source"], "currency_compact_cent_fix")

    def test_tarifa_zero_line_never_beats_valor_line(self) -> None:
        # Real OCR text (Bradesco net empresa): "Tarifa: R$ 0,00" sits right under
        # "Dados da transferencia" and used to outscore the real value.
        text = "\n".join(
            [
                "Comprovante de Transacao Bancaria",
                "Transferir",
                "bradesco",
                "Datada operacao:21/07/2026-08h38",
                "Dados da",
                "transferencia",
                "Tarifa: R$ 0,00",
                "Valor: R$ 4.917,33",
                "Midia:BRADESCONETEMPRESA",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["amount"], 4917.33)
        self.assertEqual(fields["amount_source"], "currency")

    def test_nonzero_tarifa_line_never_beats_valor_line(self) -> None:
        text = "\n".join(
            [
                "Dados da",
                "transferencia",
                "Tarifa:R$ 9,80",
                "Valor:",
                "R$90.000,00",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["amount"], 90000.0)

    def test_parses_compact_alpha_month_datetime(self) -> None:
        text = "\n".join(
            [
                "itau",
                "13mar.2026,15:44:53,viaSISPAGnoappItau",
                "Valor da transferencia",
                "R$1.680,00",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "13/03/2026")
        self.assertEqual(fields["txn_time"], "15:44")
        self.assertEqual(fields["amount"], 1680.0)

    def test_parses_numeric_date_glued_to_time(self) -> None:
        text = "\n".join(
            [
                "Comprovante de Pagamento Pix",
                "Realizada em",
                "02/02/202615:31:50",
                "Valor",
                "R$8.727,85",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "02/02/2026")
        self.assertEqual(fields["txn_time"], "15:31")
        self.assertEqual(fields["amount"], 8727.85)

    def test_parses_infinitepay_style_destination_and_alpha_month_date(self) -> None:
        text = "\n".join(
            [
                "infinitepay",
                "Comprovante de transferencia Pix",
                "R$ 600,00",
                "28 Mar,2026 14:46",
                "Origem",
                "IRIS PANTOJA SANTIAGO",
                "CPF",
                ".499.782-",
                "Instituicao",
                "CLOUDWALK IP LTDA",
                "Destino",
                "AMD REPRESENTACOES E SERVICOS LTDA",
                "CNPJ",
                "53.356.830/0001-12",
                "Instituicao",
                "BCO DO BRASIL S.A.",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "28/03/2026")
        self.assertEqual(fields["txn_time"], "14:46")
        self.assertEqual(fields["beneficiary"], "AMD REPRESENTACOES E SERVICOS LTDA")
        self.assertEqual(fields["bank"], "AMD")
        self.assertEqual(fields["amount"], 600.0)

    def test_parses_mercado_pago_superscript_cents_amount(self) -> None:
        text = "\n".join(
            [
                "Mercado Pago",
                "Comprovante de Pix",
                "19/marco/2026 as 15h22",
                "R$ 6.60102",
                "Para",
                "Cleend Intermediacao e Atacado Ltda",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "19/03/2026")
        self.assertEqual(fields["txn_time"], "15:22")
        self.assertEqual(fields["amount"], 6601.02)
        self.assertEqual(fields["amount_rounded"], 6601.0)

    def test_ignores_cpf_chunk_when_value_line_was_lost_by_ocr(self) -> None:
        text = "\n".join(
            [
                "Comprovante de",
                "transferencia",
                "19 MAR2026-18:12:43",
                "Valoi",
                "Tipo de transferencia",
                "ID da transacao",
                "E182361202026031",
                "92112s16e16aa8b0",
                "Nome",
                "AMD REPRESENTACOES E",
                "SERVICOSLTDA",
                "CNPJ",
                "53356830000112",
                "Instituicao",
                "BCO DO BRASIL S.A.",
                "Chave Pix",
                "53356830000112",
                "Origem",
                "Nome",
                "Gleisson Silva",
                "Instituicao",
                "NU PAGAMENTOS-IP",
                "CPF",
                "...300.956...",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertIsNone(fields["amount"])
        self.assertIsNone(fields["amount_rounded"])
        self.assertEqual(fields["amount_source"], "missing")

    def test_prefers_real_value_over_cpf_chunk_with_same_digits(self) -> None:
        text = "\n".join(
            [
                "Comprovante de",
                "transferencia",
                "19 MAR2026-18:12:43",
                "Valor",
                "R$29.99",
                "Tipo de transferencia",
                "Pix",
                "ID da transacao",
                "92112s16e16aa8b0",
                "Destino",
                "AMD REPRESENTACOES E",
                "SERVICOS LTDA",
                "CPF",
                "*.300.956**",
            ]
        )

        fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["amount"], 29.99)
        self.assertEqual(fields["amount_rounded"], 30.0)
        self.assertEqual(fields["amount_source"], "currency")

    def test_falls_back_to_today_and_dash_when_datetime_missing(self) -> None:
        text = "\n".join(
            [
                "Comprovante de Pix",
                "Valor do pagamento",
                "R$ 250,00",
            ]
        )

        with patch("wechat_receipt_daemon.today_local_date_str", return_value="20/03/2026"):
            fields = parse_receipt_fields(text, ocr_conf=0.99, q_score=0.95)

        self.assertEqual(fields["txn_date"], "20/03/2026")
        self.assertEqual(fields["txn_time"], "-")
        self.assertEqual(fields["txn_date_source"], "fallback_today")
        self.assertEqual(fields["txn_time_source"], "fallback_dash")


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


def build_receipt_payload(
    *,
    file_id: str,
    ingested_at: float,
    msg_svr_id: str,
    msg_create_time: float,
    amount: float,
    amount_rounded: float,
    manual_session_id: str | None = None,
) -> dict[str, object]:
    row_payload = {
        "file_id": file_id,
        "client": "65",
        "txn_date": "20/03/2026",
        "txn_time": "11:35",
        "bank": "CLEEND",
        "amount": amount_rounded,
        "verification_status": "CONFIRMADO",
        "msg_svr_id": msg_svr_id,
        "talker": "27837425841@chatroom",
    }
    return {
        "file_id": file_id,
        "source_path": f"C:/fake/{file_id}.dat",
        "source_kind": "msgattach_image_dat",
        "ingested_at": ingested_at,
        "sha256": f"sha-{file_id}",
        "txn_date": "20/03/2026",
        "txn_time": "11:35",
        "txn_date_source": "parsed",
        "txn_time_source": "parsed",
        "client": "65",
        "bank": "CLEEND",
        "beneficiary": "Cliente",
        "amount": amount,
        "amount_raw": str(amount),
        "amount_rounded": amount_rounded,
        "amount_source": "currency",
        "currency": "BRL",
        "parse_conf": 0.99,
        "quality_score": 0.95,
        "ocr_engine": "rapidocr",
        "ocr_conf": 0.99,
        "ocr_chars": 120,
        "review_needed": False,
        "ocr_text": "Comprovante de Pix",
        "parser_json": "{}",
        "msg_svr_id": msg_svr_id,
        "talker": "27837425841@chatroom",
        "msg_create_time": msg_create_time,
        "manual_session_id": manual_session_id,
        "resolved_media_path": f"C:/fake/{file_id}.dat",
        "resolution_source": "db_image",
        "verification_status": "CONFIRMADO",
        "sheet_status": "SINK_PENDING",
        "sheet_payload_json": json.dumps(row_payload),
        "sheet_next_attempt": 0.0,
        "sheet_last_error": None,
        "sheet_committed_at": None,
        "excel_sheet": None,
        "excel_row": None,
    }


def insert_file_row(
    db: StateDB,
    *,
    file_id: str,
    path: str,
    source_kind: str,
    status: str,
    first_seen: float,
    last_error: str | None,
    msg_svr_id: str | None = None,
    talker: str | None = None,
    msg_create_time: float | None = None,
    manual_session_id: str | None = None,
    session_release_at: float = 0.0,
) -> None:
    db._conn.execute(
        """
        INSERT INTO files(
            file_id, path, source_kind, ext, size, mtime, ctime, status,
            attempts, next_attempt, first_seen, last_seen, msg_svr_id, talker, msg_create_time,
            manual_session_id, session_release_at, processed_at, sha256, last_error
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
        """,
        (
            file_id,
            path,
            source_kind,
            Path(path).suffix.lower(),
            10,
            first_seen,
            first_seen,
            status,
            1,
            first_seen + 5.0,
            first_seen,
            first_seen,
            msg_svr_id,
            talker,
            msg_create_time,
            manual_session_id,
            session_release_at,
            last_error,
        ),
    )
    db._conn.commit()


class FakeMediaResolver:
    def __init__(self, messages: list[WeChatMessageRef]) -> None:
        self.messages = messages

    def list_image_messages_for_talker(
        self,
        talker: str | None,
        start_create_time: float,
        end_create_time: float,
        limit: int = 240,
    ) -> list[WeChatMessageRef]:
        talker_value = str(talker or "").strip()
        out = [
            msg
            for msg in self.messages
            if str(msg.talker or "").strip() == talker_value
            and float(start_create_time) <= float(msg.create_time) <= float(end_create_time)
        ]
        return out[:limit]

    def resolve_talker_display_name(self, talker: str | None) -> str | None:
        return str(talker or "").strip() or None


class CandidateFilterTests(unittest.TestCase):
    def test_thumb_is_ignored_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            thumb_path = root / "MsgAttach" / "gid" / "Thumb" / "2026-03" / "receipt_t.dat"
            image_path = root / "MsgAttach" / "gid" / "Image" / "2026-03" / "receipt.dat"
            temp_path = root / "FileStorage" / "Temp" / "receipt.png"
            for path in (thumb_path, image_path, temp_path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"x")

            self.assertFalse(is_candidate(thumb_path, thumb_candidates_enabled=False))
            self.assertTrue(is_candidate(thumb_path, thumb_candidates_enabled=True))
            self.assertTrue(is_candidate(image_path, thumb_candidates_enabled=False))
            self.assertTrue(is_candidate(temp_path, thumb_candidates_enabled=False))


class ReconcileScanTests(unittest.TestCase):
    def test_startup_guard_skips_existing_files_but_keeps_new_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db = StateDB(root / "state.db")
            candidate = root / "MsgAttach" / "gid" / "Image" / "2026-04" / "receipt.dat"
            candidate.parent.mkdir(parents=True)
            candidate.write_bytes(b"fake-image")
            os.utime(candidate, (1000.0, 1000.0))
            cfg = type(
                "Cfg",
                (),
                {
                    "watch_roots": [root],
                    "settle_seconds": 1,
                    "recent_files_hours": 24,
                    "thumb_candidates_enabled": False,
                    "process_existing_files_on_startup": False,
                    "startup_time": 2000.0,
                },
            )()
            try:
                with patch("wechat_receipt_daemon.time.time", return_value=2005.0):
                    first_count = reconcile_scan(cfg, db)
                self.assertEqual(first_count, 0)
                self.assertEqual(db._conn.execute("SELECT COUNT(*) FROM files").fetchone()[0], 0)

                os.utime(candidate, (2010.0, 2010.0))
                with patch("wechat_receipt_daemon.time.time", return_value=2020.0):
                    second_count = reconcile_scan(cfg, db)
                self.assertEqual(second_count, 1)
                self.assertEqual(db._conn.execute("SELECT COUNT(*) FROM files").fetchone()[0], 1)
            finally:
                db.close()


class ManualSessionOrderTests(unittest.TestCase):
    def test_manual_session_ignores_old_pending_message_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.ensure_message_job(
                    msg_svr_id="old-msg",
                    talker="27837425841@chatroom",
                    talker_display="2026 65-2群",
                    thumb_path=Path("C:/fake/old_t.dat"),
                    expected_image_path=Path("C:/fake/old.dat"),
                    create_time=100.0,
                    first_seen_at=1000.0,
                )
                db.set_message_job_state("old-msg", "WAITING_ORIGINAL", note="MANUAL_WAIT_ORIGINAL", next_ui_attempt_at=0.0)

                db.ensure_message_job(
                    msg_svr_id="new-msg",
                    talker="27837425841@chatroom",
                    talker_display="2026 65-2群",
                    thumb_path=Path("C:/fake/new_t.dat"),
                    expected_image_path=Path("C:/fake/new.dat"),
                    create_time=200.0,
                    first_seen_at=2000.0,
                )

                blocker_without_session = db.find_prior_pending_message_job(
                    talker="27837425841@chatroom",
                    create_time=200.0,
                    msg_svr_id="new-msg",
                )
                blocker_with_session = db.find_prior_pending_message_job(
                    talker="27837425841@chatroom",
                    create_time=200.0,
                    msg_svr_id="new-msg",
                    manual_session_started_at=1500.0,
                )

                self.assertIsNotNone(blocker_without_session)
                self.assertIsNone(blocker_with_session)
            finally:
                db.close()

    def test_realtime_image_event_refreshes_manual_session_but_reconcile_does_not(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            image_path = root / "MsgAttach" / "gid" / "Image" / "2026-03" / "manual.dat"
            image_path.parent.mkdir(parents=True, exist_ok=True)
            image_path.write_bytes(b"manual-open")

            db = StateDB(root / "state.db")
            try:
                db.start_manual_session(100.0)

                with patch("wechat_receipt_daemon.time.time", return_value=200.0):
                    db.upsert_candidate(
                        image_path,
                        settle_seconds=5,
                        source_event="reconcile",
                        thumb_candidates_enabled=False,
                    )
                self.assertEqual(db.get_manual_session_started_at(), 100.0)

                with patch("wechat_receipt_daemon.time.time", return_value=300.0):
                    db.upsert_candidate(
                        image_path,
                        settle_seconds=5,
                        source_event="modified",
                        thumb_candidates_enabled=False,
                    )
                self.assertEqual(db.get_manual_session_started_at(), 300.0)
            finally:
                db.close()

    def test_sink_claim_prioritizes_current_manual_session_and_preserves_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="old-file",
                        ingested_at=1000.0,
                        msg_svr_id="old-msg",
                        msg_create_time=100.0,
                        amount=450.0,
                        amount_rounded=450.0,
                    )
                )
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="current-a",
                        ingested_at=2000.0,
                        msg_svr_id="current-a-msg",
                        msg_create_time=300.0,
                        amount=668.04,
                        amount_rounded=668.0,
                    )
                )
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="current-b",
                        ingested_at=2001.0,
                        msg_svr_id="current-b-msg",
                        msg_create_time=301.0,
                        amount=700.04,
                        amount_rounded=700.0,
                    )
                )

                first_claim = db.claim_next_sink_receipt(manual_session_started_at=1500.0)
                self.assertIsNotNone(first_claim)
                self.assertEqual(first_claim["file_id"], "current-a")
                self.assertEqual(first_claim["row_payload"]["amount"], 668.0)
                db.mark_receipt_sink_committed("current-a", "Plan1", 2, committed_at=2100.0)

                second_claim = db.claim_next_sink_receipt(manual_session_started_at=1500.0)
                self.assertIsNotNone(second_claim)
                self.assertEqual(second_claim["file_id"], "current-b")
                self.assertEqual(second_claim["row_payload"]["amount"], 700.0)
            finally:
                db.close()

    def test_sink_claim_exposes_source_first_seen_for_latency_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="latency-file",
                        ingested_at=2100.0,
                        msg_svr_id="latency-msg",
                        msg_create_time=400.0,
                        amount=900.0,
                        amount_rounded=900.0,
                    )
                )
                insert_file_row(
                    db,
                    file_id="latency-file",
                    path="C:/fake/latency-file.dat",
                    source_kind="msgattach_image_dat",
                    status="done",
                    first_seen=2000.0,
                    last_error=None,
                )

                claimed = db.claim_next_sink_receipt()

                self.assertIsNotNone(claimed)
                self.assertEqual(claimed["source_first_seen"], 2000.0)
                self.assertEqual(claimed["ingested_at"], 2100.0)
            finally:
                db.close()

    def test_claim_next_orders_current_manual_session_by_opening_order(self) -> None:
        # The sheet must follow the operator's opening order (file mtime), even when
        # message send-times point the other way.
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="opened-first",
                    path="C:/fake/opened-first.dat",
                    source_kind="msgattach_image_dat",
                    status="pending",
                    first_seen=1000.0,  # helper also uses this as mtime
                    last_error=None,
                    msg_svr_id="newer-msg",
                    talker="27837425841@chatroom",
                    msg_create_time=301.0,
                    manual_session_id="session-a",
                    session_release_at=1005.0,
                )
                insert_file_row(
                    db,
                    file_id="opened-second",
                    path="C:/fake/opened-second.dat",
                    source_kind="msgattach_image_dat",
                    status="pending",
                    first_seen=1002.0,
                    last_error=None,
                    msg_svr_id="older-msg",
                    talker="27837425841@chatroom",
                    msg_create_time=300.0,
                    manual_session_id="session-a",
                    session_release_at=1005.0,
                )

                with patch("wechat_receipt_daemon.time.time", return_value=1010.0):
                    first_claim = db.claim_next(manual_session_id="session-a")
                self.assertIsNotNone(first_claim)
                self.assertEqual(first_claim.file_id, "opened-first")

                db.mark_done("opened-first", sha256="sha-old", processed_at=1010.0)
                with patch("wechat_receipt_daemon.time.time", return_value=1011.0):
                    second_claim = db.claim_next(manual_session_id="session-a")
                self.assertIsNotNone(second_claim)
                self.assertEqual(second_claim.file_id, "opened-second")
            finally:
                db.close()

    def test_claim_next_prefers_direct_image_before_temp_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="temp-file",
                    path="C:/fake/temp-file.jpg",
                    source_kind="temp_image",
                    status="pending",
                    first_seen=1000.0,
                    last_error=None,
                )
                insert_file_row(
                    db,
                    file_id="direct-file",
                    path="C:/fake/direct-file.dat",
                    source_kind="msgattach_image_dat",
                    status="pending",
                    first_seen=1001.0,
                    last_error=None,
                )

                with patch("wechat_receipt_daemon.time.time", return_value=1010.0):
                    claimed = db.claim_next()

                self.assertIsNotNone(claimed)
                self.assertEqual(claimed.file_id, "direct-file")
            finally:
                db.close()

    def test_seed_ready_manual_session_placeholders_only_within_burst_range(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                with patch("wechat_receipt_daemon.time.time", return_value=10.0):
                    session = db.start_or_extend_manual_order_session(
                        talker="27837425841@chatroom",
                        create_time=100.0,
                        event_ts=10.0,
                        burst_gap_seconds=2,
                        burst_max_seconds=8,
                    )
                self.assertIsNotNone(session)
                with patch("wechat_receipt_daemon.time.time", return_value=11.0):
                    db.start_or_extend_manual_order_session(
                        talker="27837425841@chatroom",
                        create_time=104.0,
                        event_ts=11.0,
                        burst_gap_seconds=2,
                        burst_max_seconds=8,
                        preferred_session_id=str(session["session_id"]),
                    )

                resolver = FakeMediaResolver(
                    [
                        WeChatMessageRef(
                            msg_svr_id="msg-99",
                            talker="27837425841@chatroom",
                            create_time=99.0,
                            sender_user_name=None,
                            sender_display=None,
                            image_rel_path=None,
                            thumb_rel_path=None,
                            image_abs_path=Path("C:/fake/msg-99.dat"),
                            thumb_abs_path=None,
                        ),
                        WeChatMessageRef(
                            msg_svr_id="msg-102",
                            talker="27837425841@chatroom",
                            create_time=102.0,
                            sender_user_name=None,
                            sender_display=None,
                            image_rel_path=None,
                            thumb_rel_path=None,
                            image_abs_path=Path("C:/fake/msg-102.dat"),
                            thumb_abs_path=None,
                        ),
                    ]
                )

                class Cfg:
                    manual_order_guard_enabled = True

                seeded = seed_ready_manual_session_placeholders(db, resolver, Cfg())

                self.assertEqual(seeded, 1)
                self.assertIsNone(db.get_message_job("msg-99"))
                placeholder = db.get_message_job("msg-102")
                self.assertIsNotNone(placeholder)
                self.assertEqual(placeholder["state"], SESSION_PENDING_OPEN_STATE)
                self.assertEqual(placeholder["manual_session_id"], session["session_id"])
            finally:
                db.close()

    def test_new_talker_rolls_previous_session_placeholders_and_releases_file_hold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                with patch("wechat_receipt_daemon.time.time", return_value=10.0):
                    first_session = db.start_or_extend_manual_order_session(
                        talker="27837425841@chatroom",
                        create_time=100.0,
                        event_ts=10.0,
                        burst_gap_seconds=2,
                        burst_max_seconds=8,
                    )
                self.assertIsNotNone(first_session)
                db.ensure_message_job(
                    msg_svr_id="old-msg",
                    talker="27837425841@chatroom",
                    talker_display="Grupo 65",
                    thumb_path=None,
                    expected_image_path=Path("C:/fake/old-msg.dat"),
                    create_time=100.0,
                    first_seen_at=10.0,
                    manual_session_id=str(first_session["session_id"]),
                    state=SESSION_PENDING_OPEN_STATE,
                    activation_seen_at=0.0,
                )
                insert_file_row(
                    db,
                    file_id="held-file",
                    path="C:/fake/held-file.dat",
                    source_kind="msgattach_image_dat",
                    status="retry",
                    first_seen=12.0,
                    last_error="WAITING_SESSION_PRIOR_MESSAGE_ORDER:old-msg",
                    msg_svr_id="new-msg",
                    talker="27837425841@chatroom",
                    msg_create_time=101.0,
                    manual_session_id=str(first_session["session_id"]),
                )

                with patch("wechat_receipt_daemon.time.time", return_value=20.0):
                    second_session = db.start_or_extend_manual_order_session(
                        talker="wxid_other_chat",
                        create_time=200.0,
                        event_ts=20.0,
                        burst_gap_seconds=2,
                        burst_max_seconds=8,
                    )

                self.assertIsNotNone(second_session)
                self.assertNotEqual(first_session["session_id"], second_session["session_id"])
                rolled_job = db.get_message_job("old-msg")
                self.assertIsNotNone(rolled_job)
                self.assertEqual(rolled_job["state"], IGNORED_SESSION_ROLLOVER_STATE)
                held_file = db.get_file("held-file")
                self.assertIsNotNone(held_file)
                self.assertEqual(held_file["status"], "retry")
                self.assertEqual(held_file["last_error"], IGNORED_SESSION_ROLLOVER_STATE)
            finally:
                db.close()

    def test_sink_claim_waits_for_prior_session_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.ensure_message_job(
                    msg_svr_id="older-msg",
                    talker="27837425841@chatroom",
                    talker_display="Grupo 65",
                    thumb_path=None,
                    expected_image_path=Path("C:/fake/older-msg.dat"),
                    create_time=300.0,
                    first_seen_at=1000.0,
                    manual_session_id="session-a",
                    state=SESSION_PENDING_OPEN_STATE,
                    activation_seen_at=0.0,
                )
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="newer-file",
                        ingested_at=2000.0,
                        msg_svr_id="newer-msg",
                        msg_create_time=301.0,
                        amount=668.04,
                        amount_rounded=668.0,
                        manual_session_id="session-a",
                    )
                )

                claimed = db.claim_next_sink_receipt(manual_session_id="session-a")

                self.assertIsNone(claimed)
                row = db._conn.execute(
                    """
                    SELECT sheet_status, sheet_last_error
                    FROM receipts
                    WHERE file_id='newer-file'
                    """
                ).fetchone()
                self.assertEqual(row["sheet_status"], "SINK_BLOCKED_PRIOR_MSG")
                self.assertEqual(row["sheet_last_error"], "WAITING_PRIOR_SINK_SESSION_MESSAGE:older-msg")
            finally:
                db.close()


class ManualOpenOnlyCleanupTests(unittest.TestCase):
    def test_cleanup_ignores_only_legacy_thumb_and_temp_waits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="thumb-wait",
                    path="C:/fake/thumb-wait_t.dat",
                    source_kind="msgattach_thumb_dat",
                    status="retry",
                    first_seen=1000.0,
                    last_error="MANUAL_WAIT_ORIGINAL",
                )
                insert_file_row(
                    db,
                    file_id="temp-wait",
                    path="C:/fake/temp-wait.png",
                    source_kind="temp_image",
                    status="pending",
                    first_seen=1000.0,
                    last_error="WAITING_TEMP_CONTEXT",
                )
                insert_file_row(
                    db,
                    file_id="image-keep",
                    path="C:/fake/image-keep.dat",
                    source_kind="msgattach_image_dat",
                    status="retry",
                    first_seen=1000.0,
                    last_error="WAITING_ORIGINAL_MEDIA",
                )
                insert_file_row(
                    db,
                    file_id="temp-keep",
                    path="C:/fake/temp-keep.png",
                    source_kind="temp_image",
                    status="retry",
                    first_seen=1000.0,
                    last_error="OTHER_REASON",
                )

                ignored = db.ignore_manual_open_only_waits()

                self.assertEqual(ignored, 2)
                rows = db._conn.execute(
                    """
                    SELECT file_id, status, last_error
                    FROM files
                    ORDER BY file_id ASC
                    """
                ).fetchall()
                mapped = {row["file_id"]: (row["status"], row["last_error"]) for row in rows}
                self.assertEqual(mapped["thumb-wait"], ("ignored", "IGNORED_MANUAL_OPEN_ONLY"))
                self.assertEqual(mapped["temp-wait"], ("ignored", "IGNORED_MANUAL_OPEN_ONLY"))
                self.assertEqual(mapped["image-keep"], ("retry", "WAITING_ORIGINAL_MEDIA"))
                self.assertEqual(mapped["temp-keep"], ("retry", "OTHER_REASON"))
            finally:
                db.close()

class WeChatDBResolverMergeRunnerTests(unittest.TestCase):
    def test_parse_merge_runner_output_reads_prefixed_json(self) -> None:
        output = "\n".join(
            [
                "warning line",
                "__WXMERGE__{\"code\": true, \"ret\": \"C:/tmp/merge.db\"}",
            ]
        )

        payload = WeChatDBResolver._parse_merge_runner_output(output)

        self.assertEqual(payload, {"code": True, "ret": "C:/tmp/merge.db"})

    def test_parse_merge_runner_output_returns_none_without_marker(self) -> None:
        self.assertIsNone(WeChatDBResolver._parse_merge_runner_output("warning only"))

    def _build_resolver(self, tmp_dir: str) -> WeChatDBResolver:
        resolver = WeChatDBResolver.__new__(WeChatDBResolver)
        resolver.watch_roots = []
        resolver.wx_dirs = []
        resolver.wechat_root = None
        resolver.merge_path = Path(tmp_dir) / "merge.db"
        resolver.refresh_seconds = 10
        resolver.merge_timeout_seconds = 12
        resolver.failure_backoff_seconds = 60
        resolver._pywxdump = object()
        resolver._decode_bytes_extra = object()
        resolver._wx_key = "key"
        resolver._wx_dir = Path(tmp_dir)
        resolver._last_refresh = 0.0
        resolver._last_failure = 0.0
        resolver._last_error = None
        resolver._lock = threading.Lock()
        resolver._merge_thread = None
        resolver._merge_thread_lock = threading.Lock()
        resolver._merge_started_at = 0.0
        resolver._merge_seq = 0
        resolver.merge_stall_seconds = 300
        resolver._newest_msg_cache = (0.0, 0.0)
        resolver._load_account_info = lambda force=False: True
        return resolver

    def _refresh_and_join(self, resolver, force: bool = False) -> bool:
        ret = resolver.refresh_if_due(force=force)
        if resolver._merge_thread is not None:
            resolver._merge_thread.join()
        return ret

    def test_refresh_if_due_backs_off_after_failed_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            resolver = self._build_resolver(tmp_dir)
            merge_calls: list[str] = []

            def fake_merge(target_path) -> tuple[bool, str]:
                merge_calls.append("merge")
                return False, "timeout"

            resolver._merge_real_time_db_with_timeout_path = fake_merge

            with patch("wechat_receipt_daemon.time.time", side_effect=[100.0, 100.0, 105.0, 170.0, 170.0]):
                self.assertFalse(self._refresh_and_join(resolver))
                self.assertFalse(self._refresh_and_join(resolver))
                self.assertFalse(self._refresh_and_join(resolver))

            self.assertEqual(merge_calls, ["merge", "merge"])
            self.assertEqual(resolver.last_error, "merge_failed:timeout")
            self.assertEqual(resolver._last_failure, 170.0)

    def test_refresh_if_due_clears_failure_after_successful_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            resolver = self._build_resolver(tmp_dir)
            merge_calls: list[str] = []
            results = [(False, "timeout"), (True, "ok")]

            def fake_merge(target_path) -> tuple[bool, str]:
                merge_calls.append("merge")
                ok, ret = results.pop(0)
                if ok:
                    target_path.write_text("ok", encoding="utf-8")
                return ok, ret

            resolver._merge_real_time_db_with_timeout_path = fake_merge

            with patch("wechat_receipt_daemon.time.time", side_effect=[100.0, 100.0, 170.0, 170.0, 175.0]):
                self.assertFalse(self._refresh_and_join(resolver))
                self.assertFalse(self._refresh_and_join(resolver))
                self.assertTrue(self._refresh_and_join(resolver))

            self.assertEqual(merge_calls, ["merge", "merge"])
            self.assertEqual(resolver._last_failure, 0.0)
            self.assertIsNone(resolver.last_error)

    def test_refresh_if_due_restarts_a_wedged_merge(self) -> None:
        """A merge thread that never returns used to freeze the index forever."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            resolver = self._build_resolver(tmp_dir)
            release = threading.Event()
            entered = threading.Semaphore(0)
            merge_calls: list[str] = []

            def fake_merge(target_path) -> tuple[bool, str]:
                merge_calls.append("merge")
                entered.release()
                release.wait(timeout=10)
                return True, "ok"

            resolver._merge_real_time_db_with_timeout_path = fake_merge
            wedged = None
            try:
                with patch("wechat_receipt_daemon.time.time", return_value=100.0):
                    resolver.refresh_if_due()
                wedged = resolver._merge_thread
                self.assertTrue(entered.acquire(timeout=10))
                self.assertEqual(merge_calls, ["merge"])

                # Still inside the stall window: no second attempt.
                with patch("wechat_receipt_daemon.time.time", return_value=200.0):
                    resolver.refresh_if_due(force=True)
                self.assertEqual(merge_calls, ["merge"])

                # Past it: the wedge is reported and a fresh attempt starts.
                with patch("wechat_receipt_daemon.time.time", return_value=500.0):
                    resolver.refresh_if_due(force=True)
                self.assertTrue(entered.acquire(timeout=10))
                self.assertEqual(merge_calls, ["merge", "merge"])
                self.assertIsNotNone(resolver.last_error)
                self.assertTrue(str(resolver.last_error).startswith("merge_stalled:"))
            finally:
                release.set()
                for thread in (wedged, resolver._merge_thread):
                    if thread is not None:
                        thread.join(timeout=10)


class PdfMessageCorrelationTests(unittest.TestCase):
    """WeChat hard-links a repeated document under a new '(n)' name, so every
    copy carries the mtime of the first materialization. Correlation has to key
    on the file name, not on a time window around that mtime."""

    def _build_index(self, tmp_dir: str, rows: list[tuple[int, str, int, str]]) -> Path:
        merge_path = Path(tmp_dir) / "merge.db"
        conn = sqlite3.connect(str(merge_path))
        try:
            conn.execute(
                "CREATE TABLE MSG(MsgSvrID INTEGER, StrTalker TEXT, CreateTime INTEGER,"
                " Type INTEGER, BytesExtra BLOB)"
            )
            conn.executemany(
                "INSERT INTO MSG(MsgSvrID, StrTalker, CreateTime, Type, BytesExtra) VALUES(?,?,?,49,?)",
                [(svr, talker, ts, rel.encode("utf-8")) for svr, talker, ts, rel in rows],
            )
            conn.commit()
        finally:
            conn.close()
        return merge_path

    def _build_resolver(self, merge_path: Path) -> WeChatDBResolver:
        resolver = WeChatDBResolver.__new__(WeChatDBResolver)
        resolver.wechat_root = None
        resolver.merge_path = merge_path
        resolver._decode_bytes_extra = lambda be: {"3": [{"1": "4", "2": bytes(be).decode("utf-8")}]}
        resolver.refresh_if_due = lambda force=False: True
        return resolver

    def test_finds_the_message_for_a_hard_linked_copy(self) -> None:
        base = "wxid_x\\FileStorage\\File\\2026-07\\Bradesco 22500,00 AMD"
        with tempfile.TemporaryDirectory() as tmp_dir:
            merge_path = self._build_index(
                tmp_dir,
                [
                    (11, "grupo_a@chatroom", 1785173690, f"{base}.(1).pdf"),
                    (22, "grupo_b@chatroom", 1785182400, f"{base}..pdf"),
                    (33, "grupo_c@chatroom", 1785241566, f"{base}.(2).pdf"),
                ],
            )
            resolver = self._build_resolver(merge_path)

            # The copy whose message is 15h *after* the shared inode mtime.
            found = resolver.find_file_message_by_name(Path(f"C:/wx/{Path(base).name}.(2).pdf"))
            self.assertIsNotNone(found)
            self.assertEqual(found.talker, "grupo_c@chatroom")
            self.assertEqual(found.msg_svr_id, "33")

            # The unsuffixed name must not match the "(1)"/"(2)" copies.
            found = resolver.find_file_message_by_name(Path(f"C:/wx/{Path(base).name}..pdf"))
            self.assertIsNotNone(found)
            self.assertEqual(found.talker, "grupo_b@chatroom")

    def test_returns_none_when_the_file_has_no_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            merge_path = self._build_index(
                tmp_dir, [(11, "grupo_a@chatroom", 1785173690, "wxid_x\\FileStorage\\File\\2026-07\\outro.pdf")]
            )
            resolver = self._build_resolver(merge_path)
            self.assertIsNone(resolver.find_file_message_by_name(Path("C:/wx/orfao.pdf")))


class StalePdfModifyTests(unittest.TestCase):
    def _pdf(self, tmp_dir: str, age_seconds: float) -> Path:
        path = Path(tmp_dir) / "comprovante.pdf"
        path.write_bytes(b"%PDF-1.4")
        stamp = time.time() - age_seconds
        os.utime(path, (stamp, stamp))
        return path

    def test_recent_pdf_modify_is_processed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.assertFalse(is_stale_pdf_modify(self._pdf(tmp_dir, 60)))

    def test_old_pdf_modify_is_noise(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.assertTrue(is_stale_pdf_modify(self._pdf(tmp_dir, 5 * 24 * 3600)))

    def test_non_pdf_is_never_filtered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "img.dat"
            path.write_bytes(b"x")
            stamp = time.time() - 5 * 24 * 3600
            os.utime(path, (stamp, stamp))
            self.assertFalse(is_stale_pdf_modify(path))


class OpeningOrderCommitGateTests(unittest.TestCase):
    """Receipts must reach the sheet in the operator's opening order (file mtime),
    regardless of which one finished OCR first."""

    def _seed(self, db: StateDB, file_id: str, mtime: float, status: str, ingested_at: float | None = None) -> None:
        insert_file_row(
            db,
            file_id=file_id,
            path=f"C:/fake/{file_id}.dat",
            source_kind="msgattach_image_dat",
            status=status,
            first_seen=mtime,  # helper uses this value for mtime as well
            last_error=None,
        )
        if ingested_at is not None:
            db.insert_receipt(
                build_receipt_payload(
                    file_id=file_id,
                    ingested_at=ingested_at,
                    msg_svr_id=f"msg-{file_id}",
                    msg_create_time=0.0,
                    amount=10.0,
                    amount_rounded=10.0,
                )
            )

    def test_commit_follows_opening_order_not_ingestion_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                # Opened first (mtime 100) but OCR finished last (ingested 220);
                # opened second (mtime 110) finished first (ingested 210).
                self._seed(db, "opened-first", mtime=100.0, status="done", ingested_at=220.0)
                self._seed(db, "opened-second", mtime=110.0, status="done", ingested_at=210.0)

                first = db.claim_next_sink_receipt()
                self.assertIsNotNone(first)
                self.assertEqual(first["file_id"], "opened-first")
            finally:
                db.close()

    def test_gate_waits_for_earlier_opened_file_still_processing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                now = time.time()
                # Earlier-opened image still being read; later one already staged.
                self._seed(db, "slow-first", mtime=now - 30.0, status="processing")
                self._seed(db, "fast-second", mtime=now - 20.0, status="done", ingested_at=now)

                self.assertIsNone(db.claim_next_sink_receipt())
                row = db._conn.execute(
                    "SELECT sheet_status, sheet_last_error FROM receipts WHERE file_id='fast-second'"
                ).fetchone()
                self.assertEqual(row["sheet_status"], "SINK_BLOCKED_PRIOR_MSG")
                self.assertIn("WAITING_PRIOR_OPEN_FILE:slow-first", row["sheet_last_error"])

                # Once the earlier receipt lands, the order unblocks naturally.
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="slow-first",
                        ingested_at=now,
                        msg_svr_id="msg-slow-first",
                        msg_create_time=0.0,
                        amount=5.0,
                        amount_rounded=5.0,
                    )
                )
                db.mark_done("slow-first", sha256="s", processed_at=now)
                first = db.claim_next_sink_receipt()
                self.assertIsNotNone(first)
                self.assertEqual(first["file_id"], "slow-first")
                db.mark_receipt_sink_committed("slow-first", "Página1", 2, committed_at=now)
                second = db.claim_next_sink_receipt()
                self.assertIsNotNone(second)
                self.assertEqual(second["file_id"], "fast-second")
            finally:
                db.close()

    def test_gate_releases_when_earlier_file_stalls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                now = time.time()
                # Earlier-opened candidate stuck in retry for far longer than the
                # stall window: it must stop blocking the queue.
                self._seed(db, "stuck-first", mtime=now - 600.0, status="retry")
                self._seed(db, "healthy-second", mtime=now - 20.0, status="done", ingested_at=now)

                claimed = db.claim_next_sink_receipt()
                self.assertIsNotNone(claimed)
                self.assertEqual(claimed["file_id"], "healthy-second")
            finally:
                db.close()

    def test_gate_waits_for_earlier_staged_receipt_in_retry_backoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                now = time.time()
                self._seed(db, "first-staged", mtime=now - 40.0, status="done", ingested_at=now - 5.0)
                self._seed(db, "second-staged", mtime=now - 30.0, status="done", ingested_at=now - 4.0)
                # First one hit a sink error and is backing off into the future.
                db.mark_receipt_sink_retry("first-staged", "APIError: quota", delay_sec=60)

                self.assertIsNone(db.claim_next_sink_receipt())
                row = db._conn.execute(
                    "SELECT sheet_status, sheet_last_error FROM receipts WHERE file_id='second-staged'"
                ).fetchone()
                self.assertEqual(row["sheet_status"], "SINK_BLOCKED_PRIOR_MSG")
                self.assertIn("WAITING_PRIOR_OPEN_RECEIPT:first-staged", row["sheet_last_error"])
            finally:
                db.close()


class StaleProcessingRecoveryTests(unittest.TestCase):
    def test_requeues_processing_row_without_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="stale-processing",
                    path="C:/fake/stale-processing.dat",
                    source_kind="msgattach_image_dat",
                    status="processing",
                    first_seen=100.0,
                    last_error=None,
                )

                with patch("wechat_receipt_daemon.time.time", return_value=500.0):
                    retry_count, done_count = db.recover_stale_processing(max_age_sec=120)

                self.assertEqual((retry_count, done_count), (1, 0))
                row = db.get_file("stale-processing")
                self.assertIsNotNone(row)
                self.assertEqual(row["status"], "retry")
                self.assertEqual(row["last_error"], "RECOVERED_STALE_PROCESSING")
                self.assertEqual(row["next_attempt"], 500.0)
            finally:
                db.close()

    def test_marks_processing_row_done_when_receipt_already_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                insert_file_row(
                    db,
                    file_id="stale-with-receipt",
                    path="C:/fake/stale-with-receipt.dat",
                    source_kind="msgattach_image_dat",
                    status="processing",
                    first_seen=100.0,
                    last_error=None,
                )
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="stale-with-receipt",
                        ingested_at=230.0,
                        msg_svr_id="msg-1",
                        msg_create_time=200.0,
                        amount=668.0,
                        amount_rounded=668.0,
                    )
                )

                with patch("wechat_receipt_daemon.time.time", return_value=500.0):
                    retry_count, done_count = db.recover_stale_processing(max_age_sec=120)

                self.assertEqual((retry_count, done_count), (0, 1))
                row = db.get_file("stale-with-receipt")
                self.assertIsNotNone(row)
                self.assertEqual(row["status"], "done")
                self.assertEqual(row["last_error"], "RECOVERED_PROCESSING_WITH_RECEIPT")
                self.assertEqual(row["processed_at"], 230.0)
            finally:
                db.close()


class StartupSinkBacklogTests(unittest.TestCase):
    def test_startup_backlog_marks_old_pending_receipts_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="old-staged",
                        ingested_at=1000.0,
                        msg_svr_id="old-msg",
                        msg_create_time=100.0,
                        amount=668.04,
                        amount_rounded=668.0,
                    )
                )

                ignored = db.ignore_stale_sink_receipts(older_than_ingested_at=2000.0)

                self.assertEqual(ignored, 1)
                row = db._conn.execute(
                    """
                    SELECT sheet_status, sheet_next_attempt, sheet_last_error
                    FROM receipts
                    WHERE file_id='old-staged'
                    """
                ).fetchone()
                self.assertEqual(row["sheet_status"], "SINK_SKIPPED_TERMINAL")
                self.assertEqual(row["sheet_next_attempt"], 0)
                self.assertEqual(row["sheet_last_error"], "IGNORED_STARTUP_BACKLOG")
            finally:
                db.close()


class RecordingSink:
    def __init__(self) -> None:
        self.updated_rows: list[tuple[str, int, dict[str, object], bool]] = []

    def append(self, row_payload: dict[str, object], review_needed: bool) -> tuple[str, int]:
        raise NotImplementedError

    def update_row(self, sheet_name: str, row_idx: int, row_payload: dict[str, object], review_needed: bool) -> None:
        self.updated_rows.append((sheet_name, row_idx, row_payload, review_needed))


class GoogleSheetsSinkTargetSheetTests(unittest.TestCase):
    def test_review_items_use_review_sheet_when_configured(self) -> None:
        sink = GoogleSheetsSink.__new__(GoogleSheetsSink)
        sink.review_worksheet = "Revisar"
        sink._main_sheet_title = "Pagina1"

        self.assertEqual(sink._target_sheet(review_needed=False), "Pagina1")
        self.assertEqual(sink._target_sheet(review_needed=True), "Revisar")

    def test_review_items_fall_back_to_main_sheet_when_review_sheet_matches_main(self) -> None:
        sink = GoogleSheetsSink.__new__(GoogleSheetsSink)
        sink.review_worksheet = "Pagina1"
        sink._main_sheet_title = "Pagina1"

        self.assertEqual(sink._target_sheet(review_needed=True), "Pagina1")

    def test_review_items_use_main_sheet_when_review_sheet_disabled(self) -> None:
        sink = GoogleSheetsSink.__new__(GoogleSheetsSink)
        sink.review_worksheet = None
        sink._main_sheet_title = "Pagina1"

        self.assertEqual(sink._target_sheet(review_needed=True), "Pagina1")


class ReceiptBackfillTests(unittest.TestCase):
    def test_backfill_updates_committed_receipt_payload_without_touching_sheet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                payload = build_receipt_payload(
                    file_id="legacy-file",
                    ingested_at=1000.0,
                    msg_svr_id="legacy-msg",
                    msg_create_time=100.0,
                    amount=668.04,
                    amount_rounded=668.0,
                )
                payload.update(
                    {
                        "txn_date": None,
                        "txn_time": None,
                        "txn_date_source": None,
                        "txn_time_source": None,
                        "amount_raw": None,
                        "amount_rounded": None,
                        "amount_source": None,
                        "review_needed": True,
                        "ocr_text": "\n".join(
                            [
                                "Comprovante de Pix",
                                "20/marco/2026 as 11h35.",
                                "R$ 66804",
                                "Banco Bradesco",
                            ]
                        ),
                        "sheet_status": "SINK_COMMITTED",
                        "sheet_payload_json": json.dumps(
                            {
                                "file_id": "legacy-file",
                                "client": "65",
                                "txn_date": "",
                                "txn_time": "",
                                "bank": "CLEEND",
                                "amount": None,
                                "verification_status": "CONFIRMADO",
                                "msg_svr_id": "legacy-msg",
                                "talker": "27837425841@chatroom",
                            }
                        ),
                        "excel_sheet": "Lancamentos",
                        "excel_row": 7,
                    }
                )
                db.insert_receipt(payload)

                sink = RecordingSink()
                cfg = type("Cfg", (), {"min_confidence": 0.8})()

                updated, sheet_updated, sheet_failed = backfill_missing_receipt_fields(db, sink, cfg, limit=10)

                self.assertEqual((updated, sheet_updated, sheet_failed), (1, 0, 0))
                row = db._conn.execute(
                    """
                    SELECT txn_date, txn_time, txn_date_source, txn_time_source,
                           amount, amount_raw, amount_rounded, amount_source,
                           review_needed, sheet_payload_json
                    FROM receipts
                    WHERE file_id='legacy-file'
                    """
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["txn_date"], "20/03/2026")
                self.assertEqual(row["txn_time"], "11:35")
                self.assertEqual(row["txn_date_source"], "parsed")
                self.assertEqual(row["txn_time_source"], "parsed")
                self.assertEqual(row["amount"], 668.04)
                self.assertEqual(row["amount_raw"], "66804")
                self.assertEqual(row["amount_rounded"], 668.0)
                self.assertEqual(row["amount_source"], "currency_compact_cent_fix")
                self.assertEqual(row["review_needed"], 1)

                self.assertEqual(sink.updated_rows, [])
            finally:
                db.close()


class GluedZeroCentFixTests(unittest.TestCase):
    """Glued BRL tokens ending in "00" must split cents like any other.

    Real regression 29/07/2026: Nubank "R$ 700,00" was OCR'd as "R$70000" and
    committed to the sheet as R$ 70.000,00 (client 991, user-confirmed).
    """

    def _nubank_text(self, valor_line: str) -> str:
        return "\n".join(
            [
                "Comprovantede",
                "transferencia",
                "29JUL2026-06:50:37",
                "Valor",
                valor_line,
                "Tipodetransferencia",
                "Pix",
                "E182361202026072",
                "IDdatransacao",
                "90949s0963991599",
                "Destino",
                "Nome",
                "CLEENDELETRONICOS",
                "CNPJ",
                "61964978000168",
                "Instituicao",
                "BCOBRADESCOS.A.",
                "Chave Pix",
                "+5511976266666",
                "Origem",
                "Nome",
                "MirtesdaSilva Lucena",
                "Instituicao",
                "NUPAGAMENTOS-IP",
                "CPF",
                ".256.092-.",
            ]
        )

    def test_five_digit_glued_ending_00_splits_cents(self) -> None:
        fields = parse_receipt_fields(self._nubank_text("R$70000"), ocr_conf=0.84, q_score=0.9)
        self.assertEqual(fields["amount"], 700.0)
        self.assertEqual(fields["amount_rounded"], 700.0)
        self.assertEqual(fields["amount_source"], "currency_compact_cent_fix")

    def test_other_glued_00_values_from_the_same_day(self) -> None:
        fields16 = parse_receipt_fields(self._nubank_text("R$16000"), ocr_conf=0.9, q_score=0.9)
        self.assertEqual(fields16["amount"], 160.0)
        fields95 = parse_receipt_fields(self._nubank_text("R$95000"), ocr_conf=0.9, q_score=0.9)
        self.assertEqual(fields95["amount"], 950.0)

    def test_four_digit_glued_ending_00_splits_cents(self) -> None:
        fields = parse_receipt_fields(self._nubank_text("R$6600"), ocr_conf=0.9, q_score=0.9)
        self.assertEqual(fields["amount"], 66.0)
        self.assertEqual(fields["amount_source"], "currency_compact_cent_fix")

    def test_seven_digit_fully_glued_thousands_and_cents(self) -> None:
        # "16.000,00" with every separator dropped -> "1600000".
        fields = parse_receipt_fields(self._nubank_text("R$1600000"), ocr_conf=0.9, q_score=0.9)
        self.assertEqual(fields["amount"], 16000.0)
        self.assertEqual(fields["amount_source"], "currency_compact_cent_fix")

    def test_eight_digit_glued_token_is_never_guessed(self) -> None:
        fields = parse_receipt_fields(self._nubank_text("R$12345678"), ocr_conf=0.9, q_score=0.9)
        self.assertIsNone(fields["amount"])
        self.assertEqual(fields["amount_source"], "missing")

    def test_separator_bearing_values_are_untouched(self) -> None:
        fields = parse_receipt_fields(self._nubank_text("R$30.000,00"), ocr_conf=0.9, q_score=0.9)
        self.assertEqual(fields["amount"], 30000.0)
        self.assertEqual(fields["amount_source"], "currency")

    def test_compact_cent_fix_tints_row_value_uncertain(self) -> None:
        from wechat_receipt_daemon import build_sheet_payload_from_receipt

        payload = build_sheet_payload_from_receipt(
            {
                "file_id": "f1",
                "client": "991",
                "amount": 700.0,
                "amount_rounded": 700.0,
                "amount_source": "currency_compact_cent_fix",
                "verification_status": "CONFIRMADO",
            }
        )
        self.assertTrue(payload["value_uncertain"])


class MaskedCpfNeverAmountTests(unittest.TestCase):
    """Masked payer documents must never be parsed as the transfer value.

    Real regression 29/07/2026 (client 991, sheet rows 229/232/236): Nubank
    bottom-half prints carried no "Valor" line and the masked CPF
    "...109.153-.." / "...756.313-.." was committed as R$ 109.153,00 /
    R$ 756.313,00.
    """

    def _nubank_bottom_half(self, cpf_line: str) -> str:
        return "\n".join(
            [
                "13:34",
                "l5G",
                "Nome",
                "CLEENDELETRONICOS",
                "CNPJ",
                "61964978000168",
                "Instituicao",
                "BCOBRADESCOS.A.",
                "Chave Pix",
                "+5511976266666",
                "Origem",
                "Nome",
                "Stephanya Kariny Alves deLima",
                "Instituicao",
                "NUPAGAMENTOS-IP",
                "CPF",
                cpf_line,
                "Nu Pagamentos S.A.-Instituicao de Pagamento",
            ]
        )

    def test_masked_cpf_with_leading_dots_is_not_an_amount(self) -> None:
        fields = parse_receipt_fields(self._nubank_bottom_half("...109.153-.."), ocr_conf=0.9, q_score=0.9)
        self.assertIsNone(fields["amount"])
        self.assertEqual(fields["amount_source"], "missing")

    def test_masked_cpf_variant_is_not_an_amount(self) -> None:
        fields = parse_receipt_fields(self._nubank_bottom_half("...756.313-.."), ocr_conf=0.9, q_score=0.9)
        self.assertIsNone(fields["amount"])

    def test_bottom_half_is_flagged_as_sender_continuation(self) -> None:
        from wechat_receipt_daemon import looks_like_sender_continuation

        self.assertTrue(looks_like_sender_continuation(self._nubank_bottom_half("...109.153-..")))


class ContinuationDetectionTests(unittest.TestCase):
    def test_tiny_glued_strip_is_a_fragment(self) -> None:
        from wechat_receipt_daemon import looks_like_receipt_fragment

        # Real regression 29/07/2026 (client 116A, sheet row 320): a 24-char
        # thumbnail strip was committed as a receipt with an empty value.
        self.assertTrue(looks_like_receipt_fragment("427/07/202614:05AMD47500"))

    def test_receipts_and_valued_strips_are_not_fragments(self) -> None:
        from wechat_receipt_daemon import looks_like_receipt_fragment

        self.assertFalse(looks_like_receipt_fragment("Valor do pagamento\nRS185,00\nPix"))
        self.assertFalse(looks_like_receipt_fragment("Comprovante de Pix\n29/julho/2026"))
        self.assertFalse(looks_like_receipt_fragment("27/07/2026 AMD R$ 475,00"))
        self.assertFalse(looks_like_receipt_fragment(""))

    def test_bb_bottom_half_without_origem_is_continuation(self) -> None:
        from wechat_receipt_daemon import looks_like_sender_continuation

        # Real regression 29/07/2026 (client 991, sheet row 234): the Banco do
        # Brasil bottom half labels the sender with CPF/Agencia/Conta and never
        # uses "Origem"/"Pagador"/"Remetente".
        parte2 = "\n".join(
            [
                "14:12#S0",
                "Carlos Eduardo Vieira",
                "CPF",
                "***951.393-**",
                "Agencia",
                "0124-4",
                "Conta",
                "136585-1",
                "Instituicao",
                "OOOOOOOO BCODOBRASILS.A.",
                "Informagoesadicionais",
                "ID:E0000000020260729171113822721627",
                "Recebeuum comprovante e",
                "ficou na duvida?",
            ]
        )
        self.assertTrue(looks_like_sender_continuation(parte2))
        fields = parse_receipt_fields(parte2, ocr_conf=0.9, q_score=0.9)
        self.assertIsNone(fields["amount"])

    def test_full_receipt_header_blocks_the_sender_label_branch(self) -> None:
        from wechat_receipt_daemon import looks_like_sender_continuation

        # A full receipt whose "Valor" line was lost still opens with the
        # "Comprovante de ..." header; it must NOT be silently skipped.
        text = "\n".join(
            [
                "Comprovante de transferencia",
                "CPF",
                "12345678900",
                "Agencia",
                "1234",
                "Conta",
                "56789-0",
                "Instituicao",
                "BCO EXEMPLO S.A.",
            ]
        )
        self.assertFalse(looks_like_sender_continuation(text))

    def test_full_pagbank_receipt_is_neither_fragment_nor_continuation(self) -> None:
        from wechat_receipt_daemon import (
            looks_like_receipt_fragment,
            looks_like_sender_continuation,
        )

        # Committed correctly at row 319 the same minute as the 116A strip --
        # it must keep passing untouched.
        text = "\n".join(
            [
                "Valor do pagamento",
                "RS185,00",
                "Tipodetransferencia",
                "Pix",
                "De",
                "Wellington Miranda Pinto",
                "CPF",
                "***721.657-**",
                "Instituicao",
                "PagBank(PagSeguroInternet",
            ]
        )
        self.assertFalse(looks_like_receipt_fragment(text))
        self.assertFalse(looks_like_sender_continuation(text))
        fields = parse_receipt_fields(text, ocr_conf=0.9, q_score=0.9)
        self.assertEqual(fields["amount"], 185.0)


class NoAmountNeverCommitsTests(unittest.TestCase):
    def test_amountless_parse_is_never_staged(self) -> None:
        from wechat_receipt_daemon import should_stage_receipt

        fields = parse_receipt_fields("427/07/202614:05AMD47500", ocr_conf=0.9, q_score=0.5)
        self.assertIsNone(fields["amount"])
        self.assertFalse(should_stage_receipt(fields))

    def test_valued_receipt_is_staged_even_without_bank_or_time(self) -> None:
        from wechat_receipt_daemon import should_stage_receipt

        fields = parse_receipt_fields("Valor do pagamento\nRS185,00\nPix", ocr_conf=0.9, q_score=0.5)
        self.assertEqual(fields["amount"], 185.0)
        self.assertTrue(should_stage_receipt(fields))

    def test_sink_choke_point_skips_amountless_row_terminally(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = StateDB(Path(tmp_dir) / "state.db")
            try:
                db.insert_receipt(
                    build_receipt_payload(
                        file_id="no-amount",
                        ingested_at=2100.0,
                        msg_svr_id="no-amount-msg",
                        msg_create_time=400.0,
                        amount=None,
                        amount_rounded=None,
                    )
                )
                insert_file_row(
                    db,
                    file_id="no-amount",
                    path="C:/fake/no-amount.dat",
                    source_kind="msgattach_image_dat",
                    status="done",
                    first_seen=2000.0,
                    last_error=None,
                )

                claimed = db.claim_next_sink_receipt()
                self.assertIsNotNone(claimed)
                self.assertIsNone(claimed["row_payload"].get("amount"))

                db.mark_receipt_sink_skipped_terminal(str(claimed["file_id"]), note="NO_AMOUNT_GUARD")

                self.assertIsNone(db.claim_next_sink_receipt())
                row = db._conn.execute(
                    "SELECT sheet_status, sheet_last_error FROM receipts WHERE file_id='no-amount'"
                ).fetchone()
                self.assertEqual(row["sheet_status"], "SINK_SKIPPED_TERMINAL")
                self.assertEqual(row["sheet_last_error"], "NO_AMOUNT_GUARD")
            finally:
                db.close()


class MergeSwapRetryTests(unittest.TestCase):
    def test_swap_retries_while_a_reader_holds_the_index(self) -> None:
        # Real regression 29/07/2026: one WinError 32 during the swap (a reader
        # connection held wechat_merge.db open) wedged the index for 12h. The
        # swap must retry until the reader lets go, then clear the error.
        with tempfile.TemporaryDirectory() as tmp_dir:
            resolver = WeChatDBResolverMergeRunnerTests._build_resolver(self, tmp_dir)
            resolver.merge_path.write_text("old", encoding="utf-8")

            def fake_merge(target_path) -> tuple[bool, str]:
                target_path.write_text("new", encoding="utf-8")
                return True, "ok"

            resolver._merge_real_time_db_with_timeout_path = fake_merge
            resolver._merge_seq = 1

            holder = open(resolver.merge_path, "rb")
            releaser = threading.Timer(1.2, holder.close)
            releaser.start()
            try:
                resolver._run_background_merge(started_at=100.0, force=False, seq=1)
            finally:
                releaser.cancel()
                if not holder.closed:
                    holder.close()

            self.assertEqual(resolver.merge_path.read_text(encoding="utf-8"), "new")
            self.assertIsNone(resolver.last_error)
            self.assertEqual(resolver._last_failure, 0.0)

    def test_superseded_attempt_never_overwrites_a_newer_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            resolver = WeChatDBResolverMergeRunnerTests._build_resolver(self, tmp_dir)
            resolver.merge_path.write_text("newer", encoding="utf-8")

            def fake_merge(target_path) -> tuple[bool, str]:
                target_path.write_text("stale", encoding="utf-8")
                return True, "ok"

            resolver._merge_real_time_db_with_timeout_path = fake_merge
            resolver._merge_seq = 5  # a newer attempt already started

            resolver._run_background_merge(started_at=100.0, force=False, seq=4)

            self.assertEqual(resolver.merge_path.read_text(encoding="utf-8"), "newer")


if __name__ == "__main__":
    unittest.main()
