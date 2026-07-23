#Requires AutoHotkey v2.0
#SingleInstance Force

; Teste isolado: envia a seta para a janela capturada, sem depender
; de ela continuar sendo a janela ativa.

targetHwnd := 0
targetDescription := ""
running := false
interval := 10000
sentCount := 0

^F7::
{
    global targetHwnd, targetDescription, running, sentCount

    running := false
    SetTimer(SendRightToTarget, 0)
    sentCount := 0

    targetHwnd := WinExist("A")
    if (!targetHwnd) {
        MsgBox("Não foi possível identificar a janela ativa.", "Teste em segundo plano")
        return
    }

    title := WinGetTitle("ahk_id " targetHwnd)
    className := WinGetClass("ahk_id " targetHwnd)
    targetDescription := (title != "" ? title : "(sem título)") " | classe: " className

    MsgBox(
        "Janela capturada:`n" targetDescription
        "`n`nAgora abra o Chrome ou outro programa e pressione Ctrl+F8 para iniciar.",
        "Teste em segundo plano"
    )
}

^F8::
{
    global targetHwnd, targetDescription, running, interval

    if (running)
        return

    if (!targetHwnd || !WinExist("ahk_id " targetHwnd)) {
        MsgBox(
            "A janela do comprovante ainda não foi capturada, ou foi fechada."
            "`nAbra a primeira imagem e pressione Ctrl+F7.",
            "Teste em segundo plano"
        )
        return
    }

    running := true
    SetTimer(SendRightToTarget, interval)
    TrayTip(
        "A primeira seta será enviada em 10 segundos.`nCtrl+F9 para parar.",
        "Teste em segundo plano"
    )
}

^F9::
{
    StopTest(true)
}

^F10::
{
    StopTest(false)
    ExitApp()
}

SendRightToTarget()
{
    global targetHwnd, targetDescription, running, sentCount

    if (!running)
        return

    if (!targetHwnd || !WinExist("ahk_id " targetHwnd)) {
        StopTest(false)
        MsgBox(
            "A janela capturada foi fechada ou deixou de existir. O teste foi interrompido.",
            "Teste em segundo plano"
        )
        return
    }

    try {
        ControlSend("{Right}",, "ahk_id " targetHwnd)
        sentCount += 1
    } catch as err {
        StopTest(false)
        MsgBox(
            "O WeChat não aceitou o envio em segundo plano.`n`nDetalhe: " err.Message,
            "Teste em segundo plano"
        )
    }
}

StopTest(showResult := true)
{
    global running, sentCount

    running := false
    SetTimer(SendRightToTarget, 0)

    if (showResult) {
        MsgBox(
            "Teste parado.`nSetas enviadas: " sentCount
            "`n`nO arquivo Direita.ahk original não foi alterado.",
            "Teste em segundo plano"
        )
    }
}
