import contextlib
import contextvars
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton

SUPPORTED_LANGS = {"en", "zh", "fr", "ar", "ru", "ko"}

_ACTIVE_LANG: contextvars.ContextVar[str] = contextvars.ContextVar("nadobro_active_lang", default="en")


def normalize_lang(lang: str | None) -> str:
    value = (lang or "en").strip().lower()
    return value if value in SUPPORTED_LANGS else "en"


@contextlib.contextmanager
def language_context(lang: str | None):
    token = _ACTIVE_LANG.set(normalize_lang(lang))
    try:
        yield
    finally:
        _ACTIVE_LANG.reset(token)


def get_active_language() -> str:
    return normalize_lang(_ACTIVE_LANG.get())


def get_user_language(telegram_id: int) -> str:
    # Lazy import avoids circular imports.
    from src.nadobro.services.user_service import get_user
    user = get_user(telegram_id)
    return normalize_lang(getattr(user, "language", "en"))


LANGUAGE_LABELS = {
    "en": "English",
    "zh": "中文",
    "fr": "Français",
    "ar": "العربية",
    "ru": "Русский",
    "ko": "한국어",
}


_LABELS = {
    "🏠 Home": {
        "zh": "🏠 首页",
        "fr": "🏠 Accueil",
        "ar": "🏠 الرئيسية",
        "ru": "🏠 Главная",
        "ko": "🏠 홈",
    },
    "🤖 Trade Console": {
        "zh": "🤖 交易控制台",
        "fr": "🤖 Console Trading",
        "ar": "🤖 وحدة التداول",
        "ru": "🤖 Торговая консоль",
        "ko": "🤖 트레이드 콘솔",
    },
    "📁 Portfolio Deck": {
        "zh": "📁 投资组合",
        "fr": "📁 Portefeuille",
        "ar": "📁 المحفظة",
        "ru": "📁 Портфель",
        "ko": "📁 포트폴리오",
    },
    "💼 Wallet Vault": {
        "zh": "💼 钱包",
        "fr": "💼 Coffre Wallet",
        "ar": "💼 المحفظة",
        "ru": "💼 Кошелек",
        "ko": "💼 월렛",
    },
    "🧠 Strategy Lab": {
        "zh": "🧠 策略实验室",
        "fr": "🧠 Lab Stratégie",
        "ar": "🧠 مختبر الاستراتيجيات",
        "ru": "🧠 Лаборатория стратегий",
        "ko": "🧠 전략 랩",
    },
    "🔔 Alert Engine": {
        "zh": "🔔 提醒中心",
        "fr": "🔔 Alertes",
        "ar": "🔔 التنبيهات",
        "ru": "🔔 Центр оповещений",
        "ko": "🔔 알림 엔진",
    },
    "⚙️ Control Panel": {
        "zh": "⚙️ 控制面板",
        "fr": "⚙️ Panneau de contrôle",
        "ar": "⚙️ لوحة التحكم",
        "ru": "⚙️ Панель управления",
        "ko": "⚙️ 제어 패널",
    },
    "🌐 Execution Mode": {
        "zh": "🌐 执行模式",
        "fr": "🌐 Mode d'exécution",
        "ar": "🌐 وضع التنفيذ",
        "ru": "🌐 Режим исполнения",
        "ko": "🌐 실행 모드",
    },
    "📊 My Positions": {
        "zh": "📊 我的持仓",
        "fr": "📊 Mes positions",
        "ar": "📊 مراكزي",
        "ru": "📊 Мои позиции",
        "ko": "📊 내 포지션",
    },
    "🏆 Nado Points": {
        "zh": "🏆 Nado 积分",
        "fr": "🏆 Points Nado",
        "ar": "🏆 نقاط نادو",
        "ru": "🏆 Баллы Nado",
        "ko": "🏆 나도 포인트",
    },
    "👛 Wallet": {
        "zh": "👛 钱包",
        "fr": "👛 Wallet",
        "ar": "👛 المحفظة",
        "ru": "👛 Кошелек",
        "ko": "👛 월렛",
    },
    "⚡ Strategies": {
        "zh": "⚡ 策略",
        "fr": "⚡ Stratégies",
        "ar": "⚡ الاستراتيجيات",
        "ru": "⚡ Стратегии",
        "ko": "⚡ 전략",
    },
    "❓ Help / Support": {
        "zh": "❓ 帮助 / 支持",
        "fr": "❓ Aide / Support",
        "ar": "❓ المساعدة / الدعم",
        "ru": "❓ Помощь / Поддержка",
        "ko": "❓ 도움말 / 지원",
    },
    "📌 Open Positions": {
        "zh": "📌 查看持仓",
        "fr": "📌 Positions ouvertes",
        "ar": "📌 المراكز المفتوحة",
        "ru": "📌 Открытые позиции",
        "ko": "📌 오픈 포지션",
    },
    "❌ Close All Positions": {
        "zh": "❌ 全部平仓",
        "fr": "❌ Fermer toutes les positions",
        "ar": "❌ إغلاق كل المراكز",
        "ru": "❌ Закрыть все позиции",
        "ko": "❌ 전체 포지션 종료",
    },
    "🛡 Risk Profile": {
        "zh": "🛡 风险档位",
        "fr": "🛡 Profil de risque",
        "ar": "🛡 ملف المخاطر",
        "ru": "🛡 Профиль риска",
        "ko": "🛡 리스크 프로필",
    },
    "🌐 Language": {
        "zh": "🌐 语言",
        "fr": "🌐 Langue",
        "ar": "🌐 اللغة",
        "ru": "🌐 Язык",
        "ko": "🌐 언어",
    },
    "🛡 Conservative": {
        "zh": "🛡 保守",
        "fr": "🛡 Conservateur",
        "ar": "🛡 محافظ",
        "ru": "🛡 Консервативный",
        "ko": "🛡 보수형",
    },
    "⚖️ Balanced": {
        "zh": "⚖️ 均衡",
        "fr": "⚖️ Équilibré",
        "ar": "⚖️ متوازن",
        "ru": "⚖️ Сбалансированный",
        "ko": "⚖️ 균형형",
    },
    "🔥 Aggressive": {
        "zh": "🔥 激进",
        "fr": "🔥 Agressif",
        "ar": "🔥 هجومي",
        "ru": "🔥 Агрессивный",
        "ko": "🔥 공격형",
    },
    "🤖 MM Bot": {
        "zh": "🤖 做市机器人",
        "fr": "🤖 Bot MM",
        "ar": "🤖 بوت صناعة السوق",
        "ru": "🤖 MM Бот",
        "ko": "🤖 MM 봇",
    },
    "🧮 Grid Reactor": {
        "zh": "🧮 网格策略",
        "fr": "🧮 Réacteur Grid",
        "ar": "🧮 جريد رياكتور",
        "ru": "🧮 Grid Reactor",
        "ko": "🧮 그리드 리액터",
    },
    "⚖️ Mirror DN": {
        "zh": "⚖️ 镜像 DN",
        "fr": "⚖️ Mirror DN",
        "ar": "⚖️ ميرور DN",
        "ru": "⚖️ Mirror DN",
        "ko": "⚖️ 미러 DN",
    },
    "🔁 Volume Engine": {
        "zh": "🔁 成交量引擎",
        "fr": "🔁 Moteur Volume",
        "ar": "🔁 محرك الحجم",
        "ru": "🔁 Volume Engine",
        "ko": "🔁 볼륨 엔진",
    },
    "✅ Arm Strategy": {
        "zh": "✅ 启用策略",
        "fr": "✅ Activer stratégie",
        "ar": "✅ تفعيل الاستراتيجية",
        "ru": "✅ Активировать стратегию",
        "ko": "✅ 전략 실행",
    },
    "⚙️ Tune Risk": {
        "zh": "⚙️ 调整风险",
        "fr": "⚙️ Ajuster le risque",
        "ar": "⚙️ ضبط المخاطر",
        "ru": "⚙️ Настроить риск",
        "ko": "⚙️ 리스크 조정",
    },
    "🧩 Edit Parameters": {
        "zh": "🧩 编辑参数",
        "fr": "🧩 Modifier paramètres",
        "ar": "🧩 تعديل المعلمات",
        "ru": "🧩 Параметры",
        "ko": "🧩 파라미터 편집",
    },
    "🔄 Refresh Dashboard": {
        "zh": "🔄 刷新面板",
        "fr": "🔄 Actualiser tableau",
        "ar": "🔄 تحديث اللوحة",
        "ru": "🔄 Обновить дашборд",
        "ko": "🔄 대시보드 새로고침",
    },
    "📡 Runtime Status": {
        "zh": "📡 运行状态",
        "fr": "📡 État runtime",
        "ar": "📡 حالة التشغيل",
        "ru": "📡 Статус рантайма",
        "ko": "📡 런타임 상태",
    },
    "🛑 Stop Runtime": {
        "zh": "🛑 停止运行",
        "fr": "🛑 Arrêter runtime",
        "ar": "🛑 إيقاف التشغيل",
        "ru": "🛑 Остановить рантайм",
        "ko": "🛑 런타임 중지",
    },
    "Current Mode": {
        "zh": "当前模式",
        "fr": "Mode actuel",
        "ar": "الوضع الحالي",
        "ru": "Текущий режим",
        "ko": "현재 모드",
    },
    "Scope All": {
        "zh": "全部范围",
        "fr": "Périmètre global",
        "ar": "النطاق الكلّي",
        "ru": "Весь диапазон",
        "ko": "전체 범위",
    },
    "Scope Epoch": {
        "zh": "本轮范围",
        "fr": "Périmètre époque",
        "ar": "نطاق الحقبة",
        "ru": "Диапазон эпохи",
        "ko": "에포크 범위",
    },
    "🔄 Refresh": {
        "zh": "🔄 刷新",
        "fr": "🔄 Actualiser",
        "ar": "🔄 تحديث",
        "ru": "🔄 Обновить",
        "ko": "🔄 새로고침",
    },
    "◀ Back": {
        "zh": "◀ 返回",
        "fr": "◀ Retour",
        "ar": "◀ رجوع",
        "ru": "◀ Назад",
        "ko": "◀ 뒤로",
    },
    "◀ Home": {
        "zh": "◀ 首页",
        "fr": "◀ Accueil",
        "ar": "◀ الرئيسية",
        "ru": "◀ Домой",
        "ko": "◀ 홈",
    },
    "✅ Confirm Trade": {
        "zh": "✅ 确认交易",
        "fr": "✅ Confirmer l'ordre",
        "ar": "✅ تأكيد الصفقة",
        "ru": "✅ Подтвердить сделку",
        "ko": "✅ 거래 확인",
    },
    "❌ Cancel": {
        "zh": "❌ 取消",
        "fr": "❌ Annuler",
        "ar": "❌ إلغاء",
        "ru": "❌ Отмена",
        "ko": "❌ 취소",
    },
    "🟢 Long": {
        "zh": "🟢 做多",
        "fr": "🟢 Long",
        "ar": "🟢 شراء",
        "ru": "🟢 Лонг",
        "ko": "🟢 롱",
    },
    "🔴 Short": {
        "zh": "🔴 做空",
        "fr": "🔴 Short",
        "ar": "🔴 بيع",
        "ru": "🔴 Шорт",
        "ko": "🔴 숏",
    },
    "📈 Market": {
        "zh": "📈 市价",
        "fr": "📈 Marché",
        "ar": "📈 سوق",
        "ru": "📈 Рынок",
        "ko": "📈 시장가",
    },
    "📉 Limit": {
        "zh": "📉 限价",
        "fr": "📉 Limite",
        "ar": "📉 حدّي",
        "ru": "📉 Лимит",
        "ko": "📉 지정가",
    },
    "✏️ Custom": {
        "zh": "✏️ 自定义",
        "fr": "✏️ Personnalisé",
        "ar": "✏️ مخصص",
        "ru": "✏️ Свой",
        "ko": "✏️ 사용자 지정",
    },
    "📐 Set TP/SL": {
        "zh": "📐 设置止盈/止损",
        "fr": "📐 Définir TP/SL",
        "ar": "📐 ضبط TP/SL",
        "ru": "📐 Настроить TP/SL",
        "ko": "📐 TP/SL 설정",
    },
    "⏭ Skip": {
        "zh": "⏭ 跳过",
        "fr": "⏭ Ignorer",
        "ar": "⏭ تخطي",
        "ru": "⏭ Пропустить",
        "ko": "⏭ 건너뛰기",
    },
    "Set TP": {
        "zh": "设置止盈",
        "fr": "Définir TP",
        "ar": "ضبط TP",
        "ru": "Установить TP",
        "ko": "TP 설정",
    },
    "Set SL": {
        "zh": "设置止损",
        "fr": "Définir SL",
        "ar": "ضبط SL",
        "ru": "Установить SL",
        "ko": "SL 설정",
    },
    "✅ Done": {
        "zh": "✅ 完成",
        "fr": "✅ Terminé",
        "ar": "✅ تم",
        "ru": "✅ Готово",
        "ko": "✅ 완료",
    },
    "🇬🇧 English": {
        "zh": "🇬🇧 英语",
        "fr": "🇬🇧 Anglais",
        "ar": "🇬🇧 الإنجليزية",
        "ru": "🇬🇧 Английский",
        "ko": "🇬🇧 영어",
    },
    "🇨🇳 Chinese": {
        "zh": "🇨🇳 中文",
        "fr": "🇨🇳 Chinois",
        "ar": "🇨🇳 الصينية",
        "ru": "🇨🇳 Китайский",
        "ko": "🇨🇳 중국어",
    },
    "🇫🇷 Français": {
        "zh": "🇫🇷 法语",
        "fr": "🇫🇷 Français",
        "ar": "🇫🇷 الفرنسية",
        "ru": "🇫🇷 Французский",
        "ko": "🇫🇷 프랑스어",
    },
    "🇸🇦 العربية": {
        "zh": "🇸🇦 阿拉伯语",
        "fr": "🇸🇦 Arabe",
        "ar": "🇸🇦 العربية",
        "ru": "🇸🇦 Арабский",
        "ko": "🇸🇦 아랍어",
    },
    "🇷🇺 Русский": {
        "zh": "🇷🇺 俄语",
        "fr": "🇷🇺 Russe",
        "ar": "🇷🇺 الروسية",
        "ru": "🇷🇺 Русский",
        "ko": "🇷🇺 러시아어",
    },
    "🇰🇷 Korean": {
        "zh": "🇰🇷 韩语",
        "fr": "🇰🇷 Coréen",
        "ar": "🇰🇷 الكورية",
        "ru": "🇰🇷 Корейский",
        "ko": "🇰🇷 한국어",
    },
}


_TEXTS = {
    "⚙️ *Control Panel*": {
        "zh": "⚙️ *控制面板*",
        "fr": "⚙️ *Panneau de contrôle*",
        "ar": "⚙️ *لوحة التحكم*",
        "ru": "⚙️ *Панель управления*",
        "ko": "⚙️ *제어 패널*",
    },
    "📖 *Trading Bot Guide*": {
        "zh": "📖 *交易机器人指南*",
        "fr": "📖 *Guide du bot de trading*",
        "ar": "📖 *دليل بوت التداول*",
        "ru": "📖 *Руководство торгового бота*",
        "ko": "📖 *트레이딩 봇 가이드*",
    },
    "🏆 *Your Nado Points Dashboard*": {
        "zh": "🏆 *你的 Nado 积分面板*",
        "fr": "🏆 *Votre tableau Points Nado*",
        "ar": "🏆 *لوحة نقاط Nado الخاصة بك*",
        "ru": "🏆 *Ваш дашборд баллов Nado*",
        "ko": "🏆 *내 Nado 포인트 대시보드*",
    },
    "✅ Language updated to ": {
        "zh": "✅ 语言已切换为 ",
        "fr": "✅ Langue mise à jour: ",
        "ar": "✅ تم تحديث اللغة إلى ",
        "ru": "✅ Язык изменен на ",
        "ko": "✅ 언어가 다음으로 변경됨: ",
    },
    "👋 Welcome back to Nadobro! Your trading copilot is ready.": {
        "zh": "👋 欢迎回到 Nadobro！你的交易助手已就绪。",
        "fr": "👋 Bon retour sur Nadobro ! Votre copilote de trading est prêt.",
        "ar": "👋 مرحبًا بعودتك إلى Nadobro! مساعد التداول جاهز.",
        "ru": "👋 С возвращением в Nadobro! Ваш торговый помощник готов.",
        "ko": "👋 Nadobro에 다시 오신 것을 환영합니다! 트레이딩 코파일럿이 준비되었습니다.",
    },
    "🌐 *Select Language*\n\nChoose your preferred language for onboarding and UI copy\\.": {
        "zh": "🌐 *选择语言*\n\n请选择你偏好的语言用于引导和界面\\.",
        "fr": "🌐 *Choisir la langue*\n\nChoisissez votre langue préférée pour l'onboarding et l'interface\\.",
        "ar": "🌐 *اختر اللغة*\n\nاختر لغتك المفضلة لخطوات البدء وواجهة الاستخدام\\.",
        "ru": "🌐 *Выберите язык*\n\nВыберите предпочитаемый язык для онбординга и интерфейса\\.",
        "ko": "🌐 *언어 선택*\n\n온보딩과 UI에 사용할 언어를 선택하세요\\.",
    },
    "🤖 *Nadobro Command Center*": {
        "zh": "🤖 *Nadobro 控制中心*",
        "fr": "🤖 *Centre de commande Nadobro*",
        "ar": "🤖 *مركز تحكم Nadobro*",
        "ru": "🤖 *Командный центр Nadobro*",
        "ko": "🤖 *Nadobro 커맨드 센터*",
    },
    "Use this control panel for trading, portfolio, strategy lab, and risk settings\\.": {
        "zh": "使用此控制面板进行交易、资产管理、策略实验与风险设置\\.",
        "fr": "Utilisez ce panneau pour le trading, le portefeuille, le labo stratégie et les réglages de risque\\.",
        "ar": "استخدم لوحة التحكم هذه للتداول وإدارة المحفظة ومختبر الاستراتيجيات وإعدادات المخاطر\\.",
        "ru": "Используйте эту панель для торговли, портфеля, стратегий и настроек риска\\.",
        "ko": "이 패널에서 트레이딩, 포트폴리오, 전략, 리스크 설정을 사용할 수 있습니다\\.",
    },
    "Use chat messages for AI Q\\&A and typed trade commands\\.": {
        "zh": "你也可以通过聊天进行 AI 问答并输入交易指令\\.",
        "fr": "Utilisez le chat pour les questions IA et les commandes de trading tapées\\.",
        "ar": "استخدم رسائل الدردشة لأسئلة الذكاء الاصطناعي وأوامر التداول المكتوبة\\.",
        "ru": "Используйте чат для AI-вопросов и текстовых торговых команд\\.",
        "ko": "채팅으로 AI 질의응답과 텍스트 트레이드 명령을 사용할 수 있습니다\\.",
    },
    "🌐 *Execution Mode Control*\n\nCurrent Mode:": {
        "zh": "🌐 *执行模式设置*\n\n当前模式:",
        "fr": "🌐 *Contrôle du mode d'exécution*\n\nMode actuel :",
        "ar": "🌐 *التحكم في وضع التنفيذ*\n\nالوضع الحالي:",
        "ru": "🌐 *Управление режимом исполнения*\n\nТекущий режим:",
        "ko": "🌐 *실행 모드 설정*\n\n현재 모드:",
    },
    "Switch mode below:": {
        "zh": "可在下方切换模式:",
        "fr": "Changez de mode ci-dessous :",
        "ar": "بدّل الوضع من الأسفل:",
        "ru": "Смените режим ниже:",
        "ko": "아래에서 모드를 변경하세요:",
    },
    "🤖 *Nadobro Strategy Lab*": {
        "zh": "🤖 *Nadobro 策略实验室*",
        "fr": "🤖 *Lab Stratégie Nadobro*",
        "ar": "🤖 *مختبر استراتيجيات Nadobro*",
        "ru": "🤖 *Лаборатория стратегий Nadobro*",
        "ko": "🤖 *Nadobro 전략 랩*",
    },
    "Pick a strategy to open its cockpit dashboard, tune risk, and launch with pre\\-trade analytics\\.": {
        "zh": "选择一个策略，打开控制面板、调整风险，并结合预交易分析启动\\.",
        "fr": "Choisissez une stratégie pour ouvrir son cockpit, ajuster le risque et lancer avec analytics pré-trade\\.",
        "ar": "اختر استراتيجية لفتح لوحة التحكم وضبط المخاطر والتشغيل مع تحليلات ما قبل التداول\\.",
        "ru": "Выберите стратегию, откройте ее панель, настройте риск и запустите с предторговой аналитикой\\.",
        "ko": "전략을 선택해 대시보드를 열고 리스크를 조정한 뒤 사전 분석과 함께 실행하세요\\.",
    },
    "🔔 *Alert Engine*\n\nManage your trigger alerts\\.": {
        "zh": "🔔 *提醒中心*\n\n管理你的价格提醒\\.",
        "fr": "🔔 *Moteur d'alertes*\n\nGérez vos alertes de déclenchement\\.",
        "ar": "🔔 *محرك التنبيهات*\n\nقم بإدارة تنبيهاتك\\.",
        "ru": "🔔 *Центр оповещений*\n\nУправляйте триггер-алертами\\.",
        "ko": "🔔 *알림 엔진*\n\n트리거 알림을 관리하세요\\.",
    },
    "⚠️ Something went wrong\\. Please try again\\.": {
        "zh": "⚠️ 出现错误\\，请重试\\.",
        "fr": "⚠️ Une erreur est survenue\\. Veuillez réessayer\\.",
        "ar": "⚠️ حدث خطأ\\. حاول مرة أخرى\\.",
        "ru": "⚠️ Что-то пошло не так\\. Попробуйте еще раз\\.",
        "ko": "⚠️ 문제가 발생했습니다\\. 다시 시도해 주세요\\.",
    },
    "Unknown action\\.": {
        "zh": "未知操作\\.",
        "fr": "Action inconnue\\.",
        "ar": "إجراء غير معروف\\.",
        "ru": "Неизвестное действие\\.",
        "ko": "알 수 없는 작업입니다\\.",
    },
    "⚠️ An error occurred\\. Please try again\\.": {
        "zh": "⚠️ 发生错误\\，请重试\\.",
        "fr": "⚠️ Une erreur est survenue\\. Veuillez réessayer\\.",
        "ar": "⚠️ حدث خطأ\\. حاول مرة أخرى\\.",
        "ru": "⚠️ Произошла ошибка\\. Попробуйте снова\\.",
        "ko": "⚠️ 오류가 발생했습니다\\. 다시 시도해 주세요\\.",
    },
    "Home shortcut enabled.": {
        "zh": "已启用首页快捷入口。",
        "fr": "Raccourci Accueil activé.",
        "ar": "تم تفعيل اختصار الصفحة الرئيسية.",
        "ru": "Ярлык Главная включен.",
        "ko": "홈 바로가기 활성화됨.",
    },
    "📡 *Nadobro Status*": {
        "zh": "📡 *Nadobro 状态*",
        "fr": "📡 *Statut Nadobro*",
        "ar": "📡 *حالة Nadobro*",
        "ru": "📡 *Статус Nadobro*",
        "ko": "📡 *Nadobro 상태*",
    },
    "Mode:": {
        "zh": "模式:",
        "fr": "Mode :",
        "ar": "الوضع:",
        "ru": "Режим:",
        "ko": "모드:",
    },
    "Onboarding:": {
        "zh": "引导状态:",
        "fr": "Onboarding :",
        "ar": "الإعداد:",
        "ru": "Онбординг:",
        "ko": "온보딩:",
    },
    "Next Step:": {
        "zh": "下一步:",
        "fr": "Étape suivante :",
        "ar": "الخطوة التالية:",
        "ru": "Следующий шаг:",
        "ko": "다음 단계:",
    },
    "Strategy Runtime:": {
        "zh": "策略运行:",
        "fr": "Runtime stratégie :",
        "ar": "تشغيل الاستراتيجية:",
        "ru": "Стратегия runtime:",
        "ko": "전략 런타임:",
    },
    "*Perf Snapshot*": {
        "zh": "*性能快照*",
        "fr": "*Aperçu performances*",
        "ar": "*ملخص الأداء*",
        "ru": "*Снимок производительности*",
        "ko": "*성능 스냅샷*",
    },
    "🔄 *Revoke 1CT Key (Nado)*": {
        "zh": "🔄 *撤销 1CT 密钥（Nado）*",
        "fr": "🔄 *Révoquer la clé 1CT (Nado)*",
        "ar": "🔄 *إلغاء مفتاح 1CT (Nado)*",
        "ru": "🔄 *Отозвать ключ 1CT (Nado)*",
        "ko": "🔄 *1CT 키 해제 (Nado)*",
    },
    "To close open positions, use the Positions menu.": {
        "zh": "如需平仓，请使用持仓菜单。",
        "fr": "Pour fermer des positions, utilisez le menu Positions.",
        "ar": "لإغلاق المراكز المفتوحة، استخدم قائمة المراكز.",
        "ru": "Чтобы закрыть позиции, используйте меню позиций.",
        "ko": "포지션 종료는 포지션 메뉴를 사용하세요.",
    },
    "👛 *Wallet Connect Guide*": {
        "zh": "👛 *钱包连接指南*",
        "fr": "👛 *Guide de connexion wallet*",
        "ar": "👛 *دليل ربط المحفظة*",
        "ru": "👛 *Инструкция по подключению кошелька*",
        "ko": "👛 *월렛 연결 가이드*",
    },
    "Step 1:": {
        "zh": "第 1 步:",
        "fr": "Étape 1 :",
        "ar": "الخطوة 1:",
        "ru": "Шаг 1:",
        "ko": "1단계:",
    },
    "Step 2:": {
        "zh": "第 2 步:",
        "fr": "Étape 2 :",
        "ar": "الخطوة 2:",
        "ru": "Шаг 2:",
        "ko": "2단계:",
    },
    "Step 3:": {
        "zh": "第 3 步:",
        "fr": "Étape 3 :",
        "ar": "الخطوة 3:",
        "ru": "Шаг 3:",
        "ko": "3단계:",
    },
    "Step 4:": {
        "zh": "第 4 步:",
        "fr": "Étape 4 :",
        "ar": "الخطوة 4:",
        "ru": "Шаг 4:",
        "ko": "4단계:",
    },
    "Step 5:": {
        "zh": "第 5 步:",
        "fr": "Étape 5 :",
        "ar": "الخطوة 5:",
        "ru": "Шаг 5:",
        "ko": "5단계:",
    },
    "✅ Wallet linked! Your 1CT key is encrypted and stored.": {
        "zh": "✅ 钱包已连接！你的 1CT 密钥已加密保存。",
        "fr": "✅ Wallet connecté ! Votre clé 1CT est chiffrée et enregistrée.",
        "ar": "✅ تم ربط المحفظة! تم تشفير مفتاح 1CT وحفظه.",
        "ru": "✅ Кошелек подключен! Ваш ключ 1CT зашифрован и сохранен.",
        "ko": "✅ 월렛 연결 완료! 1CT 키가 암호화되어 저장되었습니다.",
    },
    "⚠️ Session expired. Tap the Wallet button to start again.": {
        "zh": "⚠️ 会话已过期，请点击钱包按钮重新开始。",
        "fr": "⚠️ Session expirée. Appuyez sur Wallet pour recommencer.",
        "ar": "⚠️ انتهت الجلسة. اضغط زر المحفظة للبدء مجددًا.",
        "ru": "⚠️ Сессия истекла. Нажмите Wallet, чтобы начать заново.",
        "ko": "⚠️ 세션이 만료되었습니다. 월렛 버튼을 눌러 다시 시작하세요.",
    },
    "🔐 Enter your passphrase to authorize this command:": {
        "zh": "🔐 输入口令以授权此操作：",
        "fr": "🔐 Entrez votre passphrase pour autoriser cette commande :",
        "ar": "🔐 أدخل عبارة المرور لتفويض هذا الأمر:",
        "ru": "🔐 Введите пароль-фразу для авторизации команды:",
        "ko": "🔐 이 명령을 승인하려면 패스프레이즈를 입력하세요:",
    },
}


def _translate_lookup(source: dict[str, dict[str, str]], text: str, lang: str) -> str:
    if lang == "en":
        return text
    has_check = text.endswith(" ✅")
    base = text[:-2] if has_check else text
    translated = source.get(base, {}).get(lang, base)
    return f"{translated} ✅" if has_check else translated


def localize_label(text: str, lang: str | None = None) -> str:
    return _translate_lookup(_LABELS, text, normalize_lang(lang))


def localize_text(text: str, lang: str | None = None) -> str:
    selected = normalize_lang(lang)
    if selected == "en" or not text:
        return text
    out = text
    # Full-string translation first.
    out = _translate_lookup(_TEXTS, out, selected)
    # Then phrase-level translation.
    for src, targets in _TEXTS.items():
        if src in out:
            out = out.replace(src, targets.get(selected, src))
    return out


def localize_markup(markup, lang: str | None = None):
    selected = normalize_lang(lang)
    if selected == "en" or markup is None:
        return markup
    if isinstance(markup, InlineKeyboardMarkup):
        rows = []
        for row in markup.inline_keyboard:
            new_row = []
            for btn in row:
                new_row.append(
                    InlineKeyboardButton(
                        text=localize_label(btn.text, selected),
                        callback_data=btn.callback_data,
                        url=btn.url,
                        switch_inline_query=btn.switch_inline_query,
                        switch_inline_query_current_chat=btn.switch_inline_query_current_chat,
                        callback_game=btn.callback_game,
                        pay=btn.pay,
                        login_url=btn.login_url,
                        web_app=btn.web_app,
                        switch_inline_query_chosen_chat=btn.switch_inline_query_chosen_chat,
                        copy_text=btn.copy_text,
                    )
                )
            rows.append(new_row)
        return InlineKeyboardMarkup(rows)
    if isinstance(markup, ReplyKeyboardMarkup):
        rows = []
        for row in markup.keyboard:
            rows.append([KeyboardButton(localize_label(btn.text, selected)) for btn in row])
        return ReplyKeyboardMarkup(
            rows,
            resize_keyboard=markup.resize_keyboard,
            one_time_keyboard=markup.one_time_keyboard,
            selective=markup.selective,
            input_field_placeholder=markup.input_field_placeholder,
            is_persistent=markup.is_persistent,
        )
    return markup


def localize_payload(text: str | None = None, reply_markup=None, lang: str | None = None):
    selected = normalize_lang(lang)
    localized_text = localize_text(text, selected) if text is not None else text
    localized_markup = localize_markup(reply_markup, selected) if reply_markup is not None else reply_markup
    return localized_text, localized_markup

