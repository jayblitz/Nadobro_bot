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
    "⚖️ Delta Neutral": {
        "zh": "⚖️ 德尔塔中性",
        "fr": "⚖️ Delta Neutral",
        "ar": "⚖️ دلتا نيوترال",
        "ru": "⚖️ Delta Neutral",
        "ko": "⚖️ 델타 뉴트럴",
    },
    "🔁 Volume Engine": {
        "zh": "🔁 成交量引擎",
        "fr": "🔁 Moteur Volume",
        "ar": "🔁 محرك الحجم",
        "ru": "🔁 Volume Engine",
        "ko": "🔁 볼륨 엔진",
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
    "✅ Let's Get It 🔥": {
        "zh": "✅ 开始吧 🔥",
        "fr": "✅ C'est parti 🔥",
        "ar": "✅ هيا بنا 🔥",
        "ru": "✅ Поехали 🔥",
        "ko": "✅ 시작하기 🔥",
    },
    "▶ Complete setup": {
        "zh": "▶ 完成设置",
        "fr": "▶ Terminer la configuration",
        "ar": "▶ إكمال الإعداد",
        "ru": "▶ Завершить настройку",
        "ko": "▶ 설정 완료",
    },
    "Exit": {
        "zh": "退出",
        "fr": "Quitter",
        "ar": "خروج",
        "ru": "Выйти",
        "ko": "종료",
    },
    "👛 Start Wallet Setup": {
        "zh": "👛 开始钱包设置",
        "fr": "👛 Démarrer la configuration du wallet",
        "ar": "👛 بدء إعداد المحفظة",
        "ru": "👛 Начать настройку кошелька",
        "ko": "👛 월렛 설정 시작",
    },
    "🏠 Open Dashboard": {
        "zh": "🏠 打开面板",
        "fr": "🏠 Ouvrir le tableau de bord",
        "ar": "🏠 فتح لوحة التحكم",
        "ru": "🏠 Открыть дашборд",
        "ko": "🏠 대시보드 열기",
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
    "Pick a strategy to open its cockpit dashboard, edit parameters, and launch with pre\\-trade analytics\\.": {
        "zh": "选择一个策略，打开控制面板、编辑参数，并结合预交易分析启动\\.",
        "fr": "Choisissez une stratégie pour ouvrir son cockpit, modifier les paramètres et lancer avec analytics pré-trade\\.",
        "ar": "اختر استراتيجية لفتح لوحة التحكم وتعديل الإعدادات والتشغيل مع تحليلات ما قبل التداول\\.",
        "ru": "Выберите стратегию, откройте ее панель, настройте параметры и запустите с предторговой аналитикой\\.",
        "ko": "전략을 선택해 대시보드를 열고 파라미터를 수정한 뒤 사전 분석과 함께 실행하세요\\.",
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
    "Yo what’s good, future Nado whale?! 👋💰\n\nWelcome to Nadobro — the best Telegram bot for trading Perps on Nado.\n\nWe’re giving you pro tools in the palm of your hand:\n• MM Bot (Grid + RGRID that prints)\n• Delta Neutral Bot (spot + 1-5x short = easy funding)\n• Volume Bot (farm leaderboards on autopilot)\n• AI chat: just type your trade ideas in English\n\nFirst, pick your language vibe:": {
        "zh": "Yo，未来的 Nado 巨鲸你好！👋💰\n\n欢迎来到 Nadobro —— 在 Nado 上交易永续合约的顶级 Telegram 机器人。\n\n我们把专业工具放进你的掌心：\n• MM Bot（Grid + RGRID，持续输出）\n• Delta Neutral Bot（现货 + 1-5x 空单，轻松吃资金费）\n• Volume Bot（自动冲榜）\n• AI 聊天：直接输入你的交易想法\n\n先选择你的语言：",
        "fr": "Salut, futur whale Nado ! 👋💰\n\nBienvenue sur Nadobro — le meilleur bot Telegram pour trader les Perps sur Nado.\n\nOn te met des outils pro dans la main :\n• MM Bot (Grid + RGRID qui envoie)\n• Delta Neutral Bot (spot + short 1-5x = funding simplifié)\n• Volume Bot (farm des leaderboards en automatique)\n• Chat IA : tape simplement tes idées de trade\n\nD'abord, choisis ta langue :",
        "ar": "يا نجم نادو القادم! 👋💰\n\nمرحبًا بك في Nadobro — أفضل بوت تيليجرام لتداول العقود الدائمة على Nado.\n\nنضع بين يديك أدوات احترافية:\n• MM Bot (Grid + RGRID)\n• Delta Neutral Bot (سبوت + شورت 1-5x = تمويل أسهل)\n• Volume Bot (جمع النقاط تلقائيًا)\n• دردشة AI: فقط اكتب أفكارك للتداول\n\nأولًا، اختر لغتك:",
        "ru": "Привет, будущий кит Nado! 👋💰\n\nДобро пожаловать в Nadobro — лучший Telegram-бот для торговли перпами на Nado.\n\nМы даем тебе профи-инструменты прямо в руки:\n• MM Bot (Grid + RGRID)\n• Delta Neutral Bot (спот + шорт 1-5x = проще забирать фандинг)\n• Volume Bot (фарм лидбордов на автопилоте)\n• AI-чат: просто пиши идеи для сделок\n\nСначала выбери язык:",
        "ko": "안녕하세요, 미래의 Nado 고래님! 👋💰\n\nNado 퍼프 거래를 위한 최고의 텔레그램 봇 Nadobro에 오신 것을 환영합니다.\n\n손안에서 바로 쓰는 프로 도구를 제공합니다:\n• MM Bot (Grid + RGRID)\n• Delta Neutral Bot (현물 + 1-5x 숏 = 쉬운 펀딩)\n• Volume Bot (리더보드 자동 운영)\n• AI 채팅: 거래 아이디어를 바로 입력하세요\n\n먼저 사용할 언어를 선택하세요:",
    },
    "🔥 Nadobro Activated! You’re in the squad 🔥\n\nWe run on Nado’s lightning CLOB with unified margin.\n\nBy tapping \"Let’s Get It\" you accept our Terms of Use & Privacy Policy.\n\n⚡ Security First (this is why we’re better):\nWe generate a secure Linked Signer for your default subaccount only.\nYou paste the PUBLIC address into Nado Settings -> 1-Click Trading (1 tx, 5 seconds).\nYour private keys NEVER leave your wallet. Revoke anytime. 100% self-custody.\n\nReady to start printing?": {
        "zh": "🔥 Nadobro 已激活！你已加入战队 🔥\n\n我们基于 Nado 的高速 CLOB 和统一保证金运行。\n\n点击“开始吧”即表示你同意我们的使用条款和隐私政策。\n\n⚡ 安全第一（这就是我们更强的原因）：\n我们只为你的默认子账户生成安全的 Linked Signer。\n把 PUBLIC 地址粘贴到 Nado 设置 -> 1-Click Trading（1 笔交易，5 秒完成）。\n你的私钥绝不会离开钱包。可随时撤销。100% 自托管。\n\n准备好开始了吗？",
        "fr": "🔥 Nadobro activé ! Tu es dans l'équipe 🔥\n\nNous tournons sur le CLOB ultra-rapide de Nado avec marge unifiée.\n\nEn appuyant sur \"C'est parti\", tu acceptes nos Conditions d'utilisation et notre Politique de confidentialité.\n\n⚡ Sécurité d'abord (voilà pourquoi on est meilleurs) :\nNous générons un Linked Signer sécurisé uniquement pour ton sous-compte par défaut.\nTu colles l'adresse PUBLIQUE dans les réglages Nado -> 1-Click Trading (1 tx, 5 secondes).\nTes clés privées ne quittent JAMAIS ton wallet. Révocation à tout moment. 100% self-custody.\n\nPrêt à démarrer ?",
        "ar": "🔥 تم تفعيل Nadobro! أنت ضمن الفريق 🔥\n\nنعمل على CLOB السريع من Nado مع هامش موحّد.\n\nبالضغط على \"هيا بنا\" فإنك توافق على شروط الاستخدام وسياسة الخصوصية.\n\n⚡ الأمان أولًا (وهذا سبب تميزنا):\nننشئ Linked Signer آمنًا لحسابك الفرعي الافتراضي فقط.\nالصق العنوان العام في إعدادات Nado -> 1-Click Trading (معاملة واحدة، 5 ثوانٍ).\nمفاتيحك الخاصة لا تغادر محفظتك أبدًا. يمكنك الإلغاء في أي وقت. حفظ ذاتي 100%.\n\nجاهز للانطلاق؟",
        "ru": "🔥 Nadobro активирован! Ты в команде 🔥\n\nМы работаем на молниеносном CLOB от Nado с единой маржой.\n\nНажимая \"Поехали\", ты принимаешь Условия использования и Политику конфиденциальности.\n\n⚡ Безопасность прежде всего (поэтому мы лучше):\nМы создаем защищенный Linked Signer только для твоего основного субаккаунта.\nВставь ПУБЛИЧНЫЙ адрес в настройках Nado -> 1-Click Trading (1 транзакция, 5 секунд).\nПриватные ключи НИКОГДА не покидают твой кошелек. Отозвать можно в любой момент. 100% self-custody.\n\nГотов стартовать?",
        "ko": "🔥 Nadobro 활성화 완료! 이제 팀에 합류했습니다 🔥\n\nNado의 초고속 CLOB와 통합 마진 환경에서 동작합니다.\n\n\"시작하기\"를 누르면 이용약관 및 개인정보 처리방침에 동의하게 됩니다.\n\n⚡ 보안 우선 (우리가 더 나은 이유):\n기본 서브계정용 보안 Linked Signer를 생성합니다.\nNado 설정 -> 1-Click Trading에 PUBLIC 주소를 붙여 넣으세요 (1회 트랜잭션, 5초).\n개인 키는 절대 지갑을 떠나지 않습니다. 언제든지 해제 가능. 100% 셀프 커스터디.\n\n시작할 준비가 되었나요?",
    },
    "🚀 Nadobro Dashboard — You’re Live, Legend!\n\nWhat we smashing today?": {
        "zh": "🚀 Nadobro 面板已开启——你已上线！\n\n今天我们要做什么？",
        "fr": "🚀 Tableau de bord Nadobro — Tu es en ligne, champion !\n\nOn attaque quoi aujourd'hui ?",
        "ar": "🚀 لوحة Nadobro جاهزة — أنت الآن مباشر!\n\nما الذي سننجزه اليوم؟",
        "ru": "🚀 Дашборд Nadobro — ты в эфире, легенда!\n\nЧто делаем сегодня?",
        "ko": "🚀 Nadobro 대시보드 준비 완료 — 지금 라이브입니다!\n\n오늘 무엇부터 시작할까요?",
    },
    "⚠️ Complete setup first (language + accept terms).": {
        "zh": "⚠️ 请先完成设置（选择语言 + 接受条款）。",
        "fr": "⚠️ Terminez d'abord la configuration (langue + acceptation des conditions).",
        "ar": "⚠️ أكمل الإعداد أولًا (اللغة + قبول الشروط).",
        "ru": "⚠️ Сначала завершите настройку (язык + принятие условий).",
        "ko": "⚠️ 먼저 설정을 완료하세요 (언어 + 약관 동의).",
    },
    "👛 Let's connect your wallet first.\n\nBefore trading, link your signer once. Tap below to start setup.": {
        "zh": "👛 先连接你的钱包。\n\n开始交易前，请先完成一次 signer 绑定。点击下方开始设置。",
        "fr": "👛 Connectons d'abord ton wallet.\n\nAvant de trader, lie ton signer une fois. Appuie ci-dessous pour démarrer la configuration.",
        "ar": "👛 لنقم أولًا بربط محفظتك.\n\nقبل التداول، اربط الـ signer مرة واحدة. اضغط بالأسفل لبدء الإعداد.",
        "ru": "👛 Сначала подключим ваш кошелек.\n\nПеред торговлей привяжите signer один раз. Нажмите ниже, чтобы начать настройку.",
        "ko": "👛 먼저 월렛을 연결하세요.\n\n거래 전에 signer를 한 번 연결해야 합니다. 아래를 눌러 설정을 시작하세요.",
    },
    "🚀 Nadobro Command Center is live!\n\nYour trading copilot is online and ready.\nPick a module below and let's trade smarter.": {
        "zh": "🚀 Nadobro 指挥中心已上线！\n\n你的交易副驾已就绪。\n在下方选择模块，开始更聪明地交易。",
        "fr": "🚀 Le centre de commande Nadobro est en ligne !\n\nTon copilote de trading est prêt.\nChoisis un module ci-dessous et tradons plus intelligemment.",
        "ar": "🚀 مركز قيادة Nadobro أصبح مباشرًا!\n\nمساعد التداول لديك جاهز.\nاختر وحدة من الأسفل ولنبدأ تداولًا أذكى.",
        "ru": "🚀 Командный центр Nadobro уже в эфире!\n\nВаш торговый помощник онлайн и готов.\nВыберите модуль ниже и торгуйте умнее.",
        "ko": "🚀 Nadobro 커맨드 센터가 활성화되었습니다!\n\n트레이딩 코파일럿이 준비되었습니다.\n아래에서 모듈을 선택해 더 스마트하게 거래하세요.",
    },
    "⚠️ Wallet not initialized\\. Use /start first\\.": {
        "zh": "⚠️ 钱包尚未初始化\\。请先使用 /start\\。",
        "fr": "⚠️ Wallet non initialisé\\. Utilisez /start d'abord\\.",
        "ar": "⚠️ لم يتم تهيئة المحفظة بعد\\. استخدم /start أولًا\\.",
        "ru": "⚠️ Кошелек не инициализирован\\. Сначала используйте /start\\.",
        "ko": "⚠️ 월렛이 초기화되지 않았습니다\\. 먼저 /start를 사용하세요\\.",
    },
    "💰 Link your wallet first to check balance.": {
        "zh": "💰 请先连接钱包后再查看余额。",
        "fr": "💰 Connectez d'abord votre wallet pour vérifier le solde.",
        "ar": "💰 اربط محفظتك أولًا للتحقق من الرصيد.",
        "ru": "💰 Сначала подключите кошелек, чтобы проверить баланс.",
        "ko": "💰 잔액을 확인하려면 먼저 월렛을 연결하세요.",
    },
    "Could not fetch balance. Try again.": {
        "zh": "无法获取余额，请重试。",
        "fr": "Impossible de récupérer le solde. Réessayez.",
        "ar": "تعذّر جلب الرصيد. حاول مرة أخرى.",
        "ru": "Не удалось получить баланс. Попробуйте снова.",
        "ko": "잔액을 가져오지 못했습니다. 다시 시도하세요.",
    },
    "⚠️ Wallet not linked yet. Complete this quick setup to start trading.": {
        "zh": "⚠️ 钱包尚未绑定。请先完成这个快速设置以开始交易。",
        "fr": "⚠️ Wallet pas encore lié. Terminez cette configuration rapide pour commencer à trader.",
        "ar": "⚠️ المحفظة غير مرتبطة بعد. أكمل هذا الإعداد السريع لبدء التداول.",
        "ru": "⚠️ Кошелек еще не привязан. Завершите быструю настройку, чтобы начать торговать.",
        "ko": "⚠️ 월렛이 아직 연결되지 않았습니다. 빠른 설정을 완료한 뒤 거래를 시작하세요.",
    },
    "⚠️ You need a linked signer before executing trades.": {
        "zh": "⚠️ 执行交易前需要先绑定 signer。",
        "fr": "⚠️ Vous avez besoin d'un signer lié avant d'exécuter des trades.",
        "ar": "⚠️ تحتاج إلى signer مرتبط قبل تنفيذ الصفقات.",
        "ru": "⚠️ Перед исполнением сделок требуется привязанный signer.",
        "ko": "⚠️ 거래를 실행하기 전에 연결된 signer가 필요합니다.",
    },
    "⚠️ Link your wallet first to launch strategy automation.": {
        "zh": "⚠️ 启动策略自动化前请先连接钱包。",
        "fr": "⚠️ Connectez d'abord votre wallet pour lancer l'automatisation de stratégie.",
        "ar": "⚠️ اربط محفظتك أولًا لتشغيل أتمتة الاستراتيجية.",
        "ru": "⚠️ Сначала подключите кошелек, чтобы запустить автоматизацию стратегии.",
        "ko": "⚠️ 전략 자동화를 시작하려면 먼저 월렛을 연결하세요.",
    },
    "⚠️ Setup incomplete\\. Resume onboarding at ": {
        "zh": "⚠️ 设置未完成\\。请从以下步骤继续引导：",
        "fr": "⚠️ Configuration incomplète\\. Reprenez l'onboarding à l'étape ",
        "ar": "⚠️ الإعداد غير مكتمل\\. تابع الإعداد من خطوة ",
        "ru": "⚠️ Настройка не завершена\\. Продолжите онбординг с шага ",
        "ko": "⚠️ 설정이 완료되지 않았습니다\\. 다음 단계부터 온보딩을 재개하세요: ",
    },
    "↩️ Returned to home\\.": {
        "zh": "↩️ 已返回首页\\。",
        "fr": "↩️ Retour à l'accueil\\.",
        "ar": "↩️ تمت العودة إلى الصفحة الرئيسية\\.",
        "ru": "↩️ Возврат на главную\\.",
        "ko": "↩️ 홈으로 돌아왔습니다\\.",
    },
    "Use /start to open the dashboard\\.": {
        "zh": "使用 /start 打开面板\\。",
        "fr": "Utilisez /start pour ouvrir le tableau de bord\\.",
        "ar": "استخدم /start لفتح لوحة التحكم\\.",
        "ru": "Используйте /start, чтобы открыть дашборд\\.",
        "ko": "/start를 사용해 대시보드를 여세요\\.",
    },
    "Use the menu for your next action\\.": {
        "zh": "请使用菜单执行下一步操作\\。",
        "fr": "Utilisez le menu pour votre prochaine action\\.",
        "ar": "استخدم القائمة للإجراء التالي\\.",
        "ru": "Используйте меню для следующего действия\\.",
        "ko": "다음 작업은 메뉴를 사용하세요\\.",
    },
    "⚠️ Unknown action\\. Please try again\\.": {
        "zh": "⚠️ 未知操作\\。请重试\\。",
        "fr": "⚠️ Action inconnue\\. Veuillez réessayer\\.",
        "ar": "⚠️ إجراء غير معروف\\. حاول مرة أخرى\\.",
        "ru": "⚠️ Неизвестное действие\\. Попробуйте снова\\.",
        "ko": "⚠️ 알 수 없는 작업입니다\\. 다시 시도해 주세요\\.",
    },
    "❌ Close-all request cancelled\\.": {
        "zh": "❌ 已取消全部平仓请求\\。",
        "fr": "❌ Demande de fermeture totale annulée\\.",
        "ar": "❌ تم إلغاء طلب إغلاق جميع المراكز\\.",
        "ru": "❌ Запрос на закрытие всех позиций отменен\\.",
        "ko": "❌ 전체 종료 요청이 취소되었습니다\\.",
    },
    "Type `confirm` to close all positions or `cancel` to discard\\.": {
        "zh": "输入 `confirm` 以平掉全部仓位，或输入 `cancel` 取消\\。",
        "fr": "Tapez `confirm` pour fermer toutes les positions ou `cancel` pour annuler\\.",
        "ar": "اكتب `confirm` لإغلاق كل المراكز أو `cancel` للإلغاء\\.",
        "ru": "Введите `confirm`, чтобы закрыть все позиции, или `cancel`, чтобы отменить\\.",
        "ko": "모든 포지션을 닫으려면 `confirm`, 취소하려면 `cancel`을 입력하세요\\.",
    },
    "⚠️ *Close All Positions*\n\nAre you sure you want to close ALL open orders?\n\nType `confirm` to execute or `cancel` to discard\\.": {
        "zh": "⚠️ *全部平仓*\n\n确定要关闭所有未平仓订单吗？\n\n输入 `confirm` 执行，或输入 `cancel` 取消\\。",
        "fr": "⚠️ *Fermer toutes les positions*\n\nVoulez-vous vraiment fermer TOUS les ordres ouverts ?\n\nTapez `confirm` pour exécuter ou `cancel` pour annuler\\.",
        "ar": "⚠️ *إغلاق جميع المراكز*\n\nهل أنت متأكد من إغلاق جميع الأوامر المفتوحة؟\n\nاكتب `confirm` للتنفيذ أو `cancel` للإلغاء\\.",
        "ru": "⚠️ *Закрыть все позиции*\n\nВы уверены, что хотите закрыть ВСЕ открытые ордера?\n\nВведите `confirm` для выполнения или `cancel` для отмены\\.",
        "ko": "⚠️ *모든 포지션 종료*\n\n모든 오픈 주문을 종료하시겠습니까?\n\n실행하려면 `confirm`, 취소하려면 `cancel`을 입력하세요\\.",
    },
    "❌ Trade cancelled\\.": {
        "zh": "❌ 交易已取消\\。",
        "fr": "❌ Trade annulé\\.",
        "ar": "❌ تم إلغاء الصفقة\\.",
        "ru": "❌ Сделка отменена\\.",
        "ko": "❌ 거래가 취소되었습니다\\.",
    },
    "Type `confirm` to execute this trade or `cancel` to discard it\\.": {
        "zh": "输入 `confirm` 执行该交易，或输入 `cancel` 放弃\\。",
        "fr": "Tapez `confirm` pour exécuter ce trade ou `cancel` pour l'annuler\\.",
        "ar": "اكتب `confirm` لتنفيذ هذه الصفقة أو `cancel` لإلغائها\\.",
        "ru": "Введите `confirm`, чтобы выполнить эту сделку, или `cancel`, чтобы отменить\\.",
        "ko": "이 거래를 실행하려면 `confirm`, 취소하려면 `cancel`을 입력하세요\\.",
    },
    "⚠️ Link your wallet first to execute this trade.": {
        "zh": "⚠️ 请先连接钱包后再执行此交易。",
        "fr": "⚠️ Connectez d'abord votre wallet pour exécuter ce trade.",
        "ar": "⚠️ اربط محفظتك أولًا لتنفيذ هذه الصفقة.",
        "ru": "⚠️ Сначала подключите кошелек, чтобы выполнить эту сделку.",
        "ko": "⚠️ 이 거래를 실행하려면 먼저 월렛을 연결하세요.",
    },
    "⚠️ Wallet setup is required before placing text trades.": {
        "zh": "⚠️ 进行文本交易前需要先完成钱包设置。",
        "fr": "⚠️ La configuration du wallet est requise avant les trades en texte.",
        "ar": "⚠️ إعداد المحفظة مطلوب قبل تنفيذ صفقات نصية.",
        "ru": "⚠️ Перед текстовыми сделками требуется настройка кошелька.",
        "ko": "⚠️ 텍스트 거래를 하기 전에 월렛 설정이 필요합니다.",
    },
    "I can place this trade from text, but I need a bit more info\\.\n\nMissing: ": {
        "zh": "我可以根据文本下单，但还需要更多信息\\。\n\n缺少：",
        "fr": "Je peux placer ce trade depuis le texte, mais il me manque encore quelques infos\\.\n\nManquant : ",
        "ar": "يمكنني تنفيذ هذه الصفقة من النص، لكن أحتاج بعض المعلومات الإضافية\\.\n\nالمفقود: ",
        "ru": "Я могу выставить эту сделку по тексту, но мне нужно немного больше данных\\.\n\nНе хватает: ",
        "ko": "텍스트로 이 거래를 실행할 수 있지만, 정보가 조금 더 필요합니다\\.\n\n누락 항목: ",
    },
    "Example: `buy 0\\.01 BTC 5x market` or `sell 0\\.2 ETH limit 3200`": {
        "zh": "示例：`buy 0\\.01 BTC 5x market` 或 `sell 0\\.2 ETH limit 3200`",
        "fr": "Exemple : `buy 0\\.01 BTC 5x market` ou `sell 0\\.2 ETH limit 3200`",
        "ar": "مثال: `buy 0\\.01 BTC 5x market` أو `sell 0\\.2 ETH limit 3200`",
        "ru": "Пример: `buy 0\\.01 BTC 5x market` или `sell 0\\.2 ETH limit 3200`",
        "ko": "예시: `buy 0\\.01 BTC 5x market` 또는 `sell 0\\.2 ETH limit 3200`",
    },
    "Type `confirm` to execute or `cancel` to discard\\.": {
        "zh": "输入 `confirm` 执行，或输入 `cancel` 取消\\。",
        "fr": "Tapez `confirm` pour exécuter ou `cancel` pour annuler\\.",
        "ar": "اكتب `confirm` للتنفيذ أو `cancel` للإلغاء\\.",
        "ru": "Введите `confirm` для выполнения или `cancel` для отмены\\.",
        "ko": "실행하려면 `confirm`, 취소하려면 `cancel`을 입력하세요\\.",
    },
    "⌛ Trade card expired\\. Start a new guided trade\\.": {
        "zh": "⌛ 交易卡片已过期\\。请重新开始一笔引导交易\\。",
        "fr": "⌛ La carte de trade a expiré\\. Lancez un nouveau trade guidé\\.",
        "ar": "⌛ انتهت صلاحية بطاقة التداول\\. ابدأ صفقة إرشادية جديدة\\.",
        "ru": "⌛ Карточка сделки истекла\\. Начните новую пошаговую сделку\\.",
        "ko": "⌛ 트레이드 카드가 만료되었습니다\\. 새 가이드 거래를 시작하세요\\.",
    },
    "Setup incomplete. Resume onboarding at ": {
        "zh": "设置未完成。请从以下步骤继续引导：",
        "fr": "Configuration incomplète. Reprenez l'onboarding à l'étape ",
        "ar": "الإعداد غير مكتمل. تابع الإعداد من خطوة ",
        "ru": "Настройка не завершена. Продолжите онбординг с шага ",
        "ko": "설정이 완료되지 않았습니다. 다음 단계부터 온보딩을 재개하세요: ",
    },
    "Invalid size selected\\.": {
        "zh": "所选仓位大小无效\\。",
        "fr": "Taille sélectionnée invalide\\.",
        "ar": "حجم الصفقة المحدد غير صالح\\.",
        "ru": "Выбран некорректный размер\\.",
        "ko": "선택한 수량이 올바르지 않습니다\\.",
    },
    "Invalid number. Try again\\.": {
        "zh": "数字无效，请重试\\。",
        "fr": "Nombre invalide. Réessayez\\.",
        "ar": "رقم غير صالح. حاول مرة أخرى\\.",
        "ru": "Неверное число. Попробуйте снова\\.",
        "ko": "잘못된 숫자입니다. 다시 시도하세요\\.",
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
    selected = normalize_lang(lang if lang is not None else get_active_language())
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
    selected = normalize_lang(lang if lang is not None else get_active_language())
    localized_text = localize_text(text, selected) if text is not None else text
    localized_markup = localize_markup(reply_markup, selected) if reply_markup is not None else reply_markup
    return localized_text, localized_markup

