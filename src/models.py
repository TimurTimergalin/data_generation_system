"""Data models for the game simulator."""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional
from enum import Enum
import uuid


class PlayerType(str, Enum):
    """Player archetype types."""
    WHALE = "whale"
    DOLPHIN = "dolphin"
    MINNOW = "minnow"
    FREE_ENGAGED = "free_engaged"
    FREE_CASUAL = "free_casual"
    FREE_CHURNER = "free_churner"


class HeroRarity(str, Enum):
    """Hero rarity levels."""
    COMMON = "common"
    RARE = "rare"
    EPIC = "epic"
    LEGENDARY = "legendary"


class HeroClass(str, Enum):
    """Hero class types."""
    WARRIOR = "warrior"
    MAGE = "mage"
    ARCHER = "archer"
    HEALER = "healer"
    TANK = "tank"


class Platform(str, Enum):
    """Device platforms."""
    IOS = "ios"
    ANDROID = "android"


@dataclass
class HeroTemplate:
    """Template for a hero type (static data)."""
    hero_id: str
    name: str
    rarity: HeroRarity
    hero_class: HeroClass
    base_power: int


@dataclass
class HeroInstance:
    """Instance of a hero owned by a player."""
    hero_id: str
    template: HeroTemplate
    level: int = 1
    stars: int = 1
    duplicates: int = 0

    @property
    def power(self) -> int:
        """Calculate current power of the hero."""
        power_per_level = 10
        star_multiplier = 1.2
        base = self.template.base_power
        level_bonus = (self.level - 1) * power_per_level
        star_bonus = star_multiplier ** (self.stars - 1)
        return int((base + level_bonus) * star_bonus)


@dataclass
class DeviceInfo:
    """Device information for events."""
    device_id: str
    platform: Platform
    os_version: str
    app_version: str
    device_model: str
    country: str
    language: str


@dataclass
class UserProperties:
    """User properties included in every event."""
    player_level: int
    vip_level: int
    total_spent_usd: float
    days_since_install: int
    cohort_date: str  # YYYY-MM-DD
    current_chapter: int


@dataclass
class Event:
    """Base event structure."""
    event_id: str
    event_name: str
    event_timestamp: datetime
    user_id: str
    session_id: str
    device: DeviceInfo
    user_properties: UserProperties
    ab_tests: dict[str, str]
    event_properties: dict

    def to_dict(self) -> dict:
        """Convert event to dictionary for serialization."""
        return {
            "event_id": self.event_id,
            "event_name": self.event_name,
            "event_timestamp": self.event_timestamp.isoformat() + "Z",
            "user_id": self.user_id,
            "session_id": self.session_id,
            "device": {
                "device_id": self.device.device_id,
                "platform": self.device.platform.value,
                "os_version": self.device.os_version,
                "app_version": self.device.app_version,
                "device_model": self.device.device_model,
                "country": self.device.country,
                "language": self.device.language,
            },
            "user_properties": {
                "player_level": self.user_properties.player_level,
                "vip_level": self.user_properties.vip_level,
                "total_spent_usd": self.user_properties.total_spent_usd,
                "days_since_install": self.user_properties.days_since_install,
                "cohort_date": self.user_properties.cohort_date,
                "current_chapter": self.user_properties.current_chapter,
            },
            "ab_tests": self.ab_tests,
            "event_properties": self.event_properties,
        }


@dataclass
class DailyQuestProgress:
    """Progress on a daily quest."""
    quest_id: str
    quest_name: str
    target: int
    current: int
    completed: bool = False
    reward_claimed: bool = False


@dataclass
class AgentState:
    """Complete state of a player agent."""

    # Identity
    user_id: str
    device_id: str
    agent_type: PlayerType

    # Install info
    install_date: date
    install_source: str
    country: str
    platform: Platform
    device_model: str
    os_version: str
    app_version: str
    language: str = "en"

    # A/B tests
    ab_tests: dict[str, str] = field(default_factory=dict)

    # Progression
    player_level: int = 1
    player_exp: int = 0
    current_chapter: int = 1
    current_stage: int = 1
    max_chapter: int = 1
    max_stage: int = 1
    total_stages_cleared: int = 0
    tutorial_completed: bool = False
    tutorial_step: int = 0

    # Economy
    gold: int = 1000
    gems: int = 100
    summon_tickets: int = 5
    energy: int = 120
    max_energy: int = 120
    energy_last_update: Optional[datetime] = None

    # Monetization
    total_spent_usd: float = 0.0
    vip_level: int = 0
    vip_points: int = 0
    purchase_count: int = 0
    bought_starter_pack: bool = False
    has_active_monthly: bool = False
    monthly_pass_day: int = 0
    monthly_pass_start: Optional[date] = None

    # Heroes
    heroes: dict[str, HeroInstance] = field(default_factory=dict)
    team: list[str] = field(default_factory=list)  # hero_ids
    team_power: int = 0

    # Gacha
    pity_counter: int = 0
    total_gacha_pulls: int = 0

    # Social
    guild_id: Optional[str] = None
    guild_joined_date: Optional[date] = None
    arena_rank: int = 0
    arena_rating: int = 1000

    # Daily state (resets each day)
    sessions_today: int = 0
    ads_watched_today: int = 0
    arena_attempts_today: int = 5
    attacked_guild_boss_today: bool = False
    claimed_daily_login: bool = False
    claimed_idle_today: bool = False
    daily_quests: list[DailyQuestProgress] = field(default_factory=list)

    # Engagement tracking
    total_sessions: int = 0
    total_playtime_sec: int = 0
    last_session_date: Optional[date] = None
    last_session_end: Optional[datetime] = None
    login_streak: int = 0
    consecutive_losses: int = 0
    got_legendary_recently: bool = False

    # Churn
    is_churned: bool = False
    churn_date: Optional[date] = None

    # Session tracking
    current_session_id: Optional[str] = None
    current_session_start: Optional[datetime] = None
    current_session_events: int = 0
    session_stages_played: int = 0
    session_gems_spent: int = 0
    session_gold_spent: int = 0

    def get_device_info(self) -> DeviceInfo:
        """Get device info for events."""
        return DeviceInfo(
            device_id=self.device_id,
            platform=self.platform,
            os_version=self.os_version,
            app_version=self.app_version,
            device_model=self.device_model,
            country=self.country,
            language=self.language,
        )

    def get_user_properties(self, current_date: date) -> UserProperties:
        """Get user properties for events."""
        days_since = (current_date - self.install_date).days
        return UserProperties(
            player_level=self.player_level,
            vip_level=self.vip_level,
            total_spent_usd=round(self.total_spent_usd, 2),
            days_since_install=days_since,
            cohort_date=self.install_date.isoformat(),
            current_chapter=self.current_chapter,
        )

    def reset_daily_state(self) -> None:
        """Reset daily counters."""
        self.sessions_today = 0
        self.ads_watched_today = 0
        self.arena_attempts_today = 5
        self.attacked_guild_boss_today = False
        self.claimed_daily_login = False
        self.claimed_idle_today = False
        self.daily_quests = []
        self.got_legendary_recently = False

    def calculate_team_power(self) -> int:
        """Calculate total power of the team."""
        total = 0
        for hero_id in self.team:
            if hero_id in self.heroes:
                total += self.heroes[hero_id].power
        self.team_power = total
        return total

    def add_hero(self, template: HeroTemplate) -> tuple[HeroInstance, bool]:
        """Add a hero to the collection. Returns (instance, is_new)."""
        if template.hero_id in self.heroes:
            # Duplicate - add to existing
            self.heroes[template.hero_id].duplicates += 1
            return self.heroes[template.hero_id], False
        else:
            # New hero
            instance = HeroInstance(
                hero_id=template.hero_id,
                template=template,
                level=1,
                stars=1,
                duplicates=0,
            )
            self.heroes[template.hero_id] = instance

            # Add to team if space
            if len(self.team) < 5:
                self.team.append(template.hero_id)
                self.calculate_team_power()

            return instance, True

    def get_heroes_by_rarity(self) -> dict[str, int]:
        """Get count of heroes by rarity."""
        counts = {"common": 0, "rare": 0, "epic": 0, "legendary": 0}
        for hero in self.heroes.values():
            counts[hero.template.rarity.value] += 1
        return counts

    def get_max_hero_level(self) -> int:
        """Get the maximum level among all heroes."""
        if not self.heroes:
            return 0
        return max(h.level for h in self.heroes.values())

    def get_max_hero_stars(self) -> int:
        """Get the maximum stars among all heroes."""
        if not self.heroes:
            return 0
        return max(h.stars for h in self.heroes.values())


@dataclass
class Guild:
    """Guild data structure."""
    guild_id: str
    name: str
    member_count: int = 0
    max_members: int = 30
    boss_level: int = 1
    boss_hp_remaining_pct: float = 100.0

    def is_full(self) -> bool:
        """Check if guild is at capacity."""
        return self.member_count >= self.max_members


@dataclass
class GachaBanner:
    """Gacha banner data."""
    banner_id: str
    banner_type: str  # standard, limited
    featured_hero_id: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None

    def is_active(self, current_date: date) -> bool:
        """Check if banner is active on given date."""
        if self.banner_type == "standard":
            return True
        if self.start_date and self.end_date:
            return self.start_date <= current_date <= self.end_date
        return False


@dataclass
class GameEvent:
    """Temporary game event (login event, summon event, etc.)."""
    event_id: str
    event_type: str  # login_event, summon_event, spending_event, collection_event
    event_name: str
    start_date: date
    end_date: date
    milestones: list[dict] = field(default_factory=list)

    def is_active(self, current_date: date) -> bool:
        """Check if event is active on given date."""
        return self.start_date <= current_date <= self.end_date

    def days_remaining(self, current_date: date) -> int:
        """Get days remaining for the event."""
        return (self.end_date - current_date).days


def generate_event_id() -> str:
    """Generate a unique event ID."""
    return f"evt_{uuid.uuid4()}"


def generate_user_id(index: int) -> str:
    """Generate a user ID."""
    return f"u_{index:06d}"


def generate_device_id(index: int) -> str:
    """Generate a device ID."""
    return f"d_{index:06d}"


def generate_session_id() -> str:
    """Generate a session ID."""
    return f"s_{uuid.uuid4().hex[:12]}"


def generate_transaction_id(timestamp: datetime) -> str:
    """Generate a transaction ID."""
    return f"txn_{int(timestamp.timestamp() * 1000)}"
