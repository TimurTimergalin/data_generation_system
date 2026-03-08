"""Main simulation engine for data generation."""

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from random import Random
from typing import Optional, Callable

from .config import SimulationConfig
from .models import (
    AgentState,
    HeroRarity,
    PlayerType,
    generate_session_id,
)
from .agents import AgentFactory, AgentBehavior, get_ab_group
from .events import EventEmitter
from .world import WorldState
from .writers import OutputManager


# Tutorial steps definition
TUTORIAL_STEPS = [
    {"id": "tut_welcome", "name": "Welcome", "duration_range": (5, 15)},
    {"id": "tut_first_battle", "name": "First Battle", "duration_range": (20, 60)},
    {"id": "tut_hero_summon", "name": "Hero Summon", "duration_range": (20, 50)},
    {"id": "tut_hero_levelup", "name": "Hero Level Up", "duration_range": (15, 40)},
    {"id": "tut_team_setup", "name": "Team Setup", "duration_range": (15, 35)},
    {"id": "tut_campaign", "name": "Campaign Intro", "duration_range": (20, 45)},
    {"id": "tut_idle_rewards", "name": "Idle Rewards", "duration_range": (10, 25)},
    {"id": "tut_complete", "name": "Tutorial Complete", "duration_range": (5, 10)},
]

EXTENDED_TUTORIAL_STEPS = [
    {"id": "tut_arena_preview", "name": "Arena Preview", "duration_range": (20, 40)},
    {"id": "tut_shop_tour", "name": "Shop Tour", "duration_range": (15, 30)},
    {"id": "tut_guild_preview", "name": "Guild Preview", "duration_range": (15, 35)},
    {"id": "tut_advanced_tips", "name": "Advanced Tips", "duration_range": (10, 25)},
]

AD_NETWORKS = ["unity_ads", "applovin", "ironsource", "admob"]

PRODUCT_NAMES = {
    "starter_pack": "Starter Pack",
    "gems_tier1": "Pile of Gems",
    "gems_tier2": "Bag of Gems",
    "gems_tier3": "Chest of Gems",
    "gems_tier4": "Vault of Gems",
    "gems_tier5": "Treasury of Gems",
    "monthly_pass": "Monthly Pass",
}


@dataclass
class SimulationState:
    """State tracking for simulation."""
    agents: list[AgentState] = field(default_factory=list)
    active_agents: set[str] = field(default_factory=set)  # user_ids
    churned_agents: set[str] = field(default_factory=set)
    installs_per_day: list[int] = field(default_factory=list)


class Simulator:
    """Main simulation engine."""

    def __init__(
        self,
        config: SimulationConfig,
        output_manager: OutputManager,
        progress_callback: Optional[Callable[[int, int, int], None]] = None,
    ):
        self.config = config
        self.output = output_manager
        self.progress_callback = progress_callback

        self.seed = config.seed
        self.rng = Random(self.seed)

        self.world: Optional[WorldState] = None
        self.state = SimulationState()
        self.agent_factory: Optional[AgentFactory] = None
        self.behavior: Optional[AgentBehavior] = None
        self.emitter = EventEmitter()

        self.current_date: Optional[date] = None
        self.day_number = 0

    def run(self) -> None:
        """Run the full simulation."""
        # Initialize
        self._initialize()

        # Pre-calculate daily installs
        self._calculate_install_distribution()

        # Main simulation loop
        start_date = date.fromisoformat(self.config.start_date)
        duration = self.config.duration_days

        for day in range(duration):
            self.day_number = day + 1
            self.current_date = start_date + timedelta(days=day)
            self.world.current_date = self.current_date
            self.world.day_number = self.day_number

            # Simulate the day
            self._simulate_day()

            # Progress callback
            if self.progress_callback:
                total_events = self.output.get_total_events()
                self.progress_callback(self.day_number, duration, total_events)

            # Advance world state
            self.world.advance_day()

    def _initialize(self) -> None:
        """Initialize simulation components."""
        self.world = WorldState.initialize(self.config, self.rng)
        self.agent_factory = AgentFactory(self.config, self.seed)
        self.behavior = AgentBehavior(self.config)
        self.output.set_config(self.config)

    def _calculate_install_distribution(self) -> None:
        """Calculate how many installs per day."""
        total = self.config.total_installs
        duration = self.config.duration_days
        distribution = self.config.install_distribution
        decay_rate = self.config.install_decay_rate

        if distribution == "uniform":
            daily = total // duration
            self.state.installs_per_day = [daily] * duration
            # Distribute remainder
            remainder = total - (daily * duration)
            for i in range(remainder):
                self.state.installs_per_day[i] += 1

        elif distribution == "decay":
            # Exponential decay
            weights = [math.exp(-decay_rate * d) for d in range(duration)]
            total_weight = sum(weights)
            self.state.installs_per_day = [
                int(total * w / total_weight) for w in weights
            ]
            # Adjust for rounding
            diff = total - sum(self.state.installs_per_day)
            for i in range(abs(diff)):
                if diff > 0:
                    self.state.installs_per_day[i] += 1
                else:
                    self.state.installs_per_day[i] -= 1

        else:
            # Default to uniform
            daily = total // duration
            self.state.installs_per_day = [daily] * duration

        # Handle bad traffic scenario
        bad_traffic = self.config.bad_traffic_config
        if bad_traffic:
            bad_day = bad_traffic.get("day", 25) - 1
            if 0 <= bad_day < duration:
                self.state.installs_per_day[bad_day] += bad_traffic.get("volume", 0)

    def _simulate_day(self) -> None:
        """Simulate one day of the game."""
        # 1. Create new installs
        self._create_daily_installs()

        # 2. Simulate existing agents
        for agent in self.state.agents:
            if agent.is_churned:
                continue

            if self.behavior.will_return_today(agent, self.current_date, self.rng):
                self._simulate_agent_day(agent)
            else:
                # Check for permanent churn
                days_since = (self.current_date - agent.install_date).days
                churn_prob = self._get_permanent_churn_probability(agent, days_since)
                if self.rng.random() < churn_prob:
                    agent.is_churned = True
                    agent.churn_date = self.current_date
                    self.state.churned_agents.add(agent.user_id)
                    if agent.user_id in self.state.active_agents:
                        self.state.active_agents.remove(agent.user_id)

    def _create_daily_installs(self) -> None:
        """Create new player installs for the day."""
        day_index = self.day_number - 1
        num_installs = self.state.installs_per_day[day_index]

        # Check for bad traffic day
        bad_traffic = self.config.bad_traffic_config
        is_bad_traffic_day = (
            bad_traffic
            and bad_traffic.get("day") == self.day_number
        )

        normal_installs = num_installs
        bad_installs = 0

        if is_bad_traffic_day:
            bad_installs = bad_traffic.get("volume", 0)
            normal_installs = num_installs - bad_installs

        # Create normal installs
        for _ in range(normal_installs):
            source = self._select_install_source()
            agent = self.agent_factory.create_agent(
                install_date=self.current_date,
                install_source=source,
                rng=self.rng,
            )
            self.state.agents.append(agent)
            self.state.active_agents.add(agent.user_id)
            self.output.record_install(source, agent.agent_type.value)

            # First session (install day)
            self._simulate_first_session(agent)

        # Create bad traffic installs
        if bad_installs > 0:
            source_name = bad_traffic.get("source_name", "fake_network")
            bot_ratio = bad_traffic.get("bot_ratio", 0.4)

            for _ in range(bad_installs):
                is_bot = self.rng.random() < bot_ratio
                agent = self.agent_factory.create_agent(
                    install_date=self.current_date,
                    install_source=source_name,
                    rng=self.rng,
                    is_bot=is_bot,
                )
                # Apply bad traffic modifiers
                agent._source_retention_mod = bad_traffic.get("retention_modifier", 0.3)
                agent._source_monetization_mod = bad_traffic.get("monetization_modifier", 0.1)

                self.state.agents.append(agent)
                self.state.active_agents.add(agent.user_id)
                self.output.record_install(source_name, agent.agent_type.value)

                # Bot first session
                self._simulate_first_session(agent)

    def _select_install_source(self) -> str:
        """Select traffic source for a new install."""
        sources = self.config.install_sources
        value = self.rng.random()
        cumulative = 0.0

        for source, config in sources.items():
            cumulative += config["share"]
            if value < cumulative:
                return source

        return list(sources.keys())[-1]

    def _get_permanent_churn_probability(self, agent: AgentState, days_since: int) -> float:
        """Get probability of permanent churn after not returning."""
        # Higher chance of permanent churn for longer absences
        if days_since <= 7:
            return 0.1
        elif days_since <= 30:
            return 0.3
        elif days_since <= 60:
            return 0.5
        else:
            return 0.7

    def _simulate_first_session(self, agent: AgentState) -> None:
        """Simulate the first session (install + tutorial)."""
        # Session start
        session_time = self.behavior.get_session_start_time(1, self.rng)
        timestamp = datetime.combine(self.current_date, session_time.time())

        agent.current_session_id = generate_session_id()
        agent.current_session_start = timestamp
        agent.current_session_events = 0
        agent.session_stages_played = 0
        agent.session_gems_spent = 0
        agent.session_gold_spent = 0

        # Session start event
        self.emitter.emit_session_start(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            session_number=1,
            is_first_session=True,
            time_since_last_session_sec=None,
        )
        agent.current_session_events += 1

        # Tutorial
        self._simulate_tutorial(agent, timestamp)

        # Give starting heroes
        self._give_starting_heroes(agent, timestamp)

        # Daily login reward
        self._claim_daily_login(agent, timestamp)

        # End session
        duration_min = self.behavior.get_session_duration_minutes(agent, 1, self.rng)
        end_timestamp = timestamp + timedelta(minutes=duration_min)

        self.emitter.emit_session_end(
            agent=agent,
            timestamp=end_timestamp,
            current_date=self.current_date,
            session_duration_sec=duration_min * 60,
            events_count=agent.current_session_events,
            stages_played=agent.session_stages_played,
            gems_spent=agent.session_gems_spent,
            gold_spent=agent.session_gold_spent,
        )

        # Update agent state
        agent.total_sessions = 1
        agent.sessions_today = 1
        agent.total_playtime_sec = duration_min * 60
        agent.last_session_date = self.current_date
        agent.last_session_end = end_timestamp
        agent.current_session_id = None

        # Write events
        self._flush_events()

    def _simulate_tutorial(self, agent: AgentState, start_time: datetime) -> None:
        """Simulate tutorial for new player."""
        # Determine tutorial steps based on A/B test
        variant = agent.ab_tests.get("onboarding_length", "control")

        if variant == "short":
            steps = TUTORIAL_STEPS[:4]
        elif variant == "extended":
            steps = TUTORIAL_STEPS + EXTENDED_TUTORIAL_STEPS
        else:
            steps = TUTORIAL_STEPS

        current_time = start_time
        total_duration = 0
        steps_completed = 0
        steps_skipped = 0

        for i, step in enumerate(steps):
            # Chance to skip (after first 2)
            is_skipped = i >= 2 and self.rng.random() < 0.1

            if is_skipped:
                duration = self.rng.randint(1, 3)
                steps_skipped += 1
            else:
                min_dur, max_dur = step["duration_range"]
                duration = self.rng.randint(min_dur, max_dur)
                steps_completed += 1

            self.emitter.emit_tutorial_step(
                agent=agent,
                timestamp=current_time,
                current_date=self.current_date,
                step_id=step["id"],
                step_number=i + 1,
                step_name=step["name"],
                duration_sec=duration,
                is_skipped=is_skipped,
            )
            agent.current_session_events += 1

            current_time += timedelta(seconds=duration)
            total_duration += duration
            agent.tutorial_step = i + 1

        # Tutorial complete event
        self.emitter.emit_tutorial_complete(
            agent=agent,
            timestamp=current_time,
            current_date=self.current_date,
            total_duration_sec=total_duration,
            steps_completed=steps_completed,
            steps_skipped=steps_skipped,
        )
        agent.current_session_events += 1
        agent.tutorial_completed = True

    def _give_starting_heroes(self, agent: AgentState, timestamp: datetime) -> None:
        """Give starting heroes to new player."""
        # 3 common heroes to start
        common_heroes = self.world.get_heroes_by_rarity(HeroRarity.COMMON)
        for _ in range(3):
            hero_template = self.rng.choice(common_heroes)
            agent.add_hero(hero_template)

        agent.calculate_team_power()

    def _simulate_agent_day(self, agent: AgentState) -> None:
        """Simulate one day for an existing agent."""
        # Reset daily state
        agent.reset_daily_state()

        # Generate daily quests
        agent.daily_quests = self.behavior.generate_daily_quests(agent, self.rng)

        # Check for late game A/B test activation
        days_since = (self.current_date - agent.install_date).days
        if days_since >= 30 and "late_game_offer" not in agent.ab_tests:
            test_config = self.config.ab_tests.get("late_game_offer", {})
            if test_config.get("enabled", False):
                variants = test_config.get("variants", [])
                weights = test_config.get("weights", [])
                if variants and weights:
                    variant = get_ab_group(
                        agent.user_id, "late_game_offer", variants, weights, self.seed
                    )
                    agent.ab_tests["late_game_offer"] = variant

        # Update login streak
        if agent.last_session_date:
            if (self.current_date - agent.last_session_date).days == 1:
                agent.login_streak += 1
            else:
                agent.login_streak = 1
        else:
            agent.login_streak = 1

        # Determine number of sessions
        num_sessions = self.behavior.get_sessions_count(agent, self.current_date, self.rng)

        # Generate session times
        session_times = []
        for i in range(num_sessions):
            time = self.behavior.get_session_start_time(i + 1, self.rng)
            session_times.append(datetime.combine(self.current_date, time.time()))

        # Sort by time
        session_times.sort()

        # Simulate each session
        for i, session_start in enumerate(session_times):
            self._simulate_session(agent, session_start, i + 1)

        # Daily snapshot (first session)
        if num_sessions > 0:
            self.emitter.emit_player_state_snapshot(
                agent=agent,
                timestamp=session_times[0],
                current_date=self.current_date,
            )
            self._flush_events()

    def _simulate_session(
        self,
        agent: AgentState,
        start_time: datetime,
        session_number: int,
    ) -> None:
        """Simulate one session for an agent."""
        agent.total_sessions += 1
        agent.sessions_today += 1

        # Calculate time since last session
        time_since_last = None
        if agent.last_session_end:
            delta = start_time - agent.last_session_end
            time_since_last = int(delta.total_seconds())

        # Session setup
        agent.current_session_id = generate_session_id()
        agent.current_session_start = start_time
        agent.current_session_events = 0
        agent.session_stages_played = 0
        agent.session_gems_spent = 0
        agent.session_gold_spent = 0

        # Session start event
        self.emitter.emit_session_start(
            agent=agent,
            timestamp=start_time,
            current_date=self.current_date,
            session_number=agent.total_sessions,
            is_first_session=False,
            time_since_last_session_sec=time_since_last,
        )
        agent.current_session_events += 1

        current_time = start_time
        duration_min = self.behavior.get_session_duration_minutes(agent, session_number, self.rng)
        end_time = start_time + timedelta(minutes=duration_min)

        # Session actions in priority order
        if session_number == 1:
            # First session of day
            current_time = self._claim_idle_rewards(agent, current_time)
            current_time = self._claim_daily_login(agent, current_time)
            current_time = self._claim_monthly_pass(agent, current_time)

        # Main gameplay loop
        remaining_time = (end_time - current_time).total_seconds() / 60

        while remaining_time > 1:
            action_time = self.rng.randint(10, 60)  # seconds per action

            # Try different actions
            action_taken = False

            # Campaign stages
            if agent.energy >= self.config.stage_energy_cost and self.rng.random() < 0.85:
                current_time = self._play_stage(agent, current_time)
                action_taken = True

            # Hero upgrades
            elif self.rng.random() < 0.70:
                current_time = self._upgrade_hero(agent, current_time)
                action_taken = True

            # Gacha
            elif self.behavior.should_do_gacha(agent, self.rng):
                current_time = self._do_gacha(agent, current_time)
                action_taken = True

            # Arena
            elif self.behavior.should_do_arena(agent, self.rng):
                current_time = self._do_arena(agent, current_time)
                action_taken = True

            # Guild boss
            elif self.behavior.should_attack_guild_boss(agent, self.rng):
                current_time = self._attack_guild_boss(agent, current_time)
                action_taken = True

            # Guild join
            elif self.behavior.should_join_guild(agent, self.rng):
                current_time = self._join_guild(agent, current_time)
                action_taken = True

            # Watch ads
            elif self.behavior.should_watch_ad(agent, self.rng):
                current_time = self._watch_ad(agent, current_time)
                action_taken = True

            # Shop browse / IAP
            elif self.rng.random() < 0.30:
                current_time = self._browse_shop(agent, current_time)
                action_taken = True

            if not action_taken:
                current_time += timedelta(seconds=action_time)

            remaining_time = (end_time - current_time).total_seconds() / 60

            # Safety check
            if current_time >= end_time:
                break

        # Session end
        actual_duration = int((current_time - start_time).total_seconds())

        self.emitter.emit_session_end(
            agent=agent,
            timestamp=current_time,
            current_date=self.current_date,
            session_duration_sec=actual_duration,
            events_count=agent.current_session_events,
            stages_played=agent.session_stages_played,
            gems_spent=agent.session_gems_spent,
            gold_spent=agent.session_gold_spent,
        )

        # Update agent
        agent.total_playtime_sec += actual_duration
        agent.last_session_date = self.current_date
        agent.last_session_end = current_time
        agent.current_session_id = None

        # Flush events
        self._flush_events()

    def _claim_idle_rewards(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Claim idle rewards."""
        if agent.claimed_idle_today:
            return timestamp

        # Calculate idle time
        if agent.last_session_end:
            idle_hours = (timestamp - agent.last_session_end).total_seconds() / 3600
        else:
            idle_hours = 12  # Max for first claim

        max_stage = (agent.max_chapter - 1) * self.config.stages_per_chapter + agent.max_stage
        rewards = self.world.get_idle_rewards(max_stage, idle_hours)

        if rewards["gold"] > 0:
            agent.gold += rewards["gold"]
            agent.player_exp += rewards["exp"]

            max_stage_id = f"ch{agent.max_chapter:02d}_st{agent.max_stage:02d}"

            self.emitter.emit_idle_reward_claim(
                agent=agent,
                timestamp=timestamp,
                current_date=self.current_date,
                idle_duration_sec=int(rewards["hours"] * 3600),
                gold_earned=rewards["gold"],
                exp_earned=rewards["exp"],
                max_stage_id=max_stage_id,
            )
            agent.current_session_events += 1

            self.emitter.emit_economy_source(
                agent=agent,
                timestamp=timestamp,
                current_date=self.current_date,
                currency="gold",
                amount=rewards["gold"],
                balance_after=agent.gold,
                source="idle_reward",
            )
            agent.current_session_events += 1

            # Check level up
            self._check_level_up(agent, timestamp)

        agent.claimed_idle_today = True
        return timestamp + timedelta(seconds=self.rng.randint(5, 15))

    def _claim_daily_login(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Claim daily login reward."""
        if agent.claimed_daily_login:
            return timestamp

        # Calculate reward
        reward_day = (agent.login_streak - 1) % 30 + 1
        is_streak_bonus = agent.login_streak % 7 == 0

        if is_streak_bonus:
            reward_currency = "gems"
            reward_amount = 50 * (agent.login_streak // 7)
        else:
            reward_currency = "gold"
            reward_amount = 100 * reward_day

        # Apply reward
        if reward_currency == "gems":
            agent.gems += reward_amount
        else:
            agent.gold += reward_amount

        self.emitter.emit_daily_login(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            login_streak=agent.login_streak,
            reward_day=reward_day,
            reward_currency=reward_currency,
            reward_amount=reward_amount,
            is_streak_bonus=is_streak_bonus,
        )
        agent.current_session_events += 1

        self.emitter.emit_economy_source(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            currency=reward_currency,
            amount=reward_amount,
            balance_after=agent.gems if reward_currency == "gems" else agent.gold,
            source="login_reward",
        )
        agent.current_session_events += 1

        agent.claimed_daily_login = True
        return timestamp + timedelta(seconds=self.rng.randint(3, 10))

    def _claim_monthly_pass(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Claim monthly pass daily reward."""
        if not agent.has_active_monthly:
            return timestamp

        # Check if pass is still active
        if agent.monthly_pass_start:
            days_active = (self.current_date - agent.monthly_pass_start).days
            if days_active >= 30:
                agent.has_active_monthly = False
                return timestamp

            # Claim daily gems
            daily_gems = self.config.shop_products.get("monthly_pass", {}).get("gems_daily", 100)
            agent.gems += daily_gems
            agent.monthly_pass_day += 1

            self.emitter.emit_economy_source(
                agent=agent,
                timestamp=timestamp,
                current_date=self.current_date,
                currency="gems",
                amount=daily_gems,
                balance_after=agent.gems,
                source="vip_bonus",
            )
            agent.current_session_events += 1

        return timestamp + timedelta(seconds=self.rng.randint(2, 5))

    def _play_stage(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Play a campaign stage."""
        chapter = agent.current_chapter
        stage = agent.current_stage

        # Check energy
        energy_cost = self.config.stage_energy_cost
        if agent.energy < energy_cost:
            return timestamp

        # Spend energy
        agent.energy -= energy_cost
        agent.session_stages_played += 1

        self.emitter.emit_economy_sink(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            currency="energy",
            amount=energy_cost,
            balance_after=agent.energy,
            sink="stage_entry",
            sink_id=f"ch{chapter:02d}_st{stage:02d}",
        )
        agent.current_session_events += 1

        # Get required power
        required_power = self.world.get_stage_power_requirement(chapter, stage)

        # Stage start
        self.emitter.emit_stage_start(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            chapter=chapter,
            stage=stage,
            attempt_number=1,
            team_power=agent.team_power,
            team_size=len(agent.team),
            hero_ids=agent.team.copy(),
        )
        agent.current_session_events += 1

        # Simulate result
        success, stars = self.behavior.simulate_stage_result(agent, required_power, self.rng)
        duration = self.rng.randint(30, 120)  # seconds

        end_time = timestamp + timedelta(seconds=duration)

        if success:
            is_first = (chapter > agent.max_chapter) or (chapter == agent.max_chapter and stage > agent.max_stage)
            rewards = self.world.get_stage_rewards(chapter, stage)

            agent.gold += rewards["gold"]
            agent.player_exp += rewards["exp"]
            agent.total_stages_cleared += 1

            # Generate random loot
            loot = []
            if self.rng.random() < 0.3:
                loot.append({"item_id": f"equip_{self.rng.randint(1, 50):03d}", "item_type": "equipment"})

            self.emitter.emit_stage_complete(
                agent=agent,
                timestamp=end_time,
                current_date=self.current_date,
                chapter=chapter,
                stage=stage,
                duration_sec=duration,
                stars=stars,
                is_first_clear=is_first,
                gold_reward=rewards["gold"],
                exp_reward=rewards["exp"],
                loot_items=loot,
            )
            agent.current_session_events += 1

            self.emitter.emit_economy_source(
                agent=agent,
                timestamp=end_time,
                current_date=self.current_date,
                currency="gold",
                amount=rewards["gold"],
                balance_after=agent.gold,
                source="stage_reward",
                source_id=f"ch{chapter:02d}_st{stage:02d}",
            )
            agent.current_session_events += 1

            # Progress
            if is_first:
                if stage < self.config.stages_per_chapter:
                    agent.current_stage = stage + 1
                    agent.max_stage = stage + 1
                else:
                    if chapter < self.config.total_chapters:
                        agent.current_chapter = chapter + 1
                        agent.current_stage = 1
                        agent.max_chapter = chapter + 1
                        agent.max_stage = 1

            agent.consecutive_losses = 0

            # Check level up
            self._check_level_up(agent, end_time)
        else:
            self.emitter.emit_stage_fail(
                agent=agent,
                timestamp=end_time,
                current_date=self.current_date,
                chapter=chapter,
                stage=stage,
                duration_sec=duration,
                fail_reason="defeat",
                team_power=agent.team_power,
                required_power=required_power,
            )
            agent.current_session_events += 1
            agent.consecutive_losses += 1

        return end_time

    def _upgrade_hero(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Upgrade a hero."""
        if not agent.heroes:
            return timestamp

        # Find best hero to upgrade
        best_hero = None
        best_cost = float("inf")

        for hero in agent.heroes.values():
            if hero.level >= 100:
                continue
            cost = self.world.get_levelup_cost(hero.level)
            if cost <= agent.gold and cost < best_cost:
                best_hero = hero
                best_cost = cost

        if not best_hero:
            return timestamp

        # Upgrade
        old_level = best_hero.level
        old_power = best_hero.power
        agent.gold -= best_cost
        agent.session_gold_spent += best_cost
        best_hero.level += 1

        self.emitter.emit_economy_sink(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            currency="gold",
            amount=best_cost,
            balance_after=agent.gold,
            sink="hero_levelup",
            sink_id=best_hero.hero_id,
        )
        agent.current_session_events += 1

        self.emitter.emit_hero_levelup(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            hero=best_hero,
            old_level=old_level,
            new_level=best_hero.level,
            gold_spent=best_cost,
            power_before=old_power,
            power_after=best_hero.power,
        )
        agent.current_session_events += 1

        # Update team power
        agent.calculate_team_power()

        # Update daily quest
        for quest in agent.daily_quests:
            if quest.quest_id == "dq_levelup" and not quest.completed:
                quest.current += 1
                if quest.current >= quest.target:
                    quest.completed = True

        return timestamp + timedelta(seconds=self.rng.randint(5, 15))

    def _do_gacha(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Perform gacha summon."""
        pull_info = self.behavior.get_gacha_pull_type(agent, self.rng)

        if pull_info["type"] == "none":
            return timestamp

        banner = self.world.get_standard_banner()
        limited = self.world.get_limited_banner()
        if limited and self.rng.random() < 0.6:
            banner = limited

        # Banner view
        self.emitter.emit_gacha_banner_view(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            banner=banner,
            player_gems=agent.gems,
            player_tickets=agent.summon_tickets,
            can_afford_single=agent.gems >= self.config.gacha_single_cost or agent.summon_tickets >= 1,
            can_afford_multi=agent.gems >= self.config.gacha_multi_cost or agent.summon_tickets >= 10,
        )
        agent.current_session_events += 1

        timestamp += timedelta(seconds=self.rng.randint(3, 10))

        # Perform pulls
        num_pulls = pull_info["count"]
        currency = pull_info["currency"]

        # Calculate cost
        if currency == "tickets":
            cost = num_pulls
            agent.summon_tickets -= cost
        else:
            cost = self.config.gacha_multi_cost if num_pulls == 10 else self.config.gacha_single_cost
            agent.gems -= cost
            agent.session_gems_spent += cost

        # Emit cost event
        self.emitter.emit_economy_sink(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            currency="summon_tickets" if currency == "tickets" else "gems",
            amount=cost,
            balance_after=agent.summon_tickets if currency == "tickets" else agent.gems,
            sink="gacha_summon",
        )
        agent.current_session_events += 1

        # Do each pull
        for i in range(num_pulls):
            pity_before = agent.pity_counter
            rarity = self.behavior.roll_gacha(agent, self.rng)

            # Get hero
            heroes_pool = self.world.get_heroes_by_rarity(rarity)
            hero_template = self.rng.choice(heroes_pool)

            # Featured hero chance
            if banner.featured_hero_id and rarity == HeroRarity.LEGENDARY:
                if self.rng.random() < 0.5:  # 50% to be featured
                    featured = self.world.get_hero_template(banner.featured_hero_id)
                    if featured:
                        hero_template = featured

            # Add to collection
            hero_instance, is_new = agent.add_hero(hero_template)

            # Update pity
            pity_triggered = False
            if rarity == HeroRarity.LEGENDARY:
                pity_triggered = pity_before >= self.config.soft_pity_start
                agent.pity_counter = 0
                agent.got_legendary_recently = True
            else:
                agent.pity_counter += 1

            agent.total_gacha_pulls += 1

            self.emitter.emit_gacha_summon(
                agent=agent,
                timestamp=timestamp,
                current_date=self.current_date,
                banner=banner,
                summon_type="multi_10" if num_pulls == 10 else "single",
                summon_index=i + 1,
                summon_cost_currency="summon_tickets" if currency == "tickets" else "gems",
                summon_cost_amount=cost if i == 0 else 0,
                hero=hero_template,
                is_new=is_new,
                is_duplicate=not is_new,
                pity_counter_before=pity_before,
                pity_counter_after=agent.pity_counter,
                pity_triggered=pity_triggered,
            )
            agent.current_session_events += 1

            timestamp += timedelta(seconds=self.rng.randint(1, 3))

        # Update team power
        agent.calculate_team_power()

        # Update daily quest
        for quest in agent.daily_quests:
            if quest.quest_id == "dq_gacha" and not quest.completed:
                quest.current += num_pulls
                if quest.current >= quest.target:
                    quest.completed = True

        return timestamp

    def _do_arena(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Do arena battle."""
        is_paid = agent.arena_attempts_today <= 0

        if is_paid:
            if agent.gems < self.config.arena_attempt_cost_gems:
                return timestamp
            agent.gems -= self.config.arena_attempt_cost_gems
            agent.session_gems_spent += self.config.arena_attempt_cost_gems

            self.emitter.emit_economy_sink(
                agent=agent,
                timestamp=timestamp,
                current_date=self.current_date,
                currency="gems",
                amount=self.config.arena_attempt_cost_gems,
                balance_after=agent.gems,
                sink="arena_attempt",
            )
            agent.current_session_events += 1
        else:
            agent.arena_attempts_today -= 1

        # Generate opponent
        opponent_power = int(agent.team_power * self.rng.uniform(0.8, 1.2))
        opponent_rank = max(1, agent.arena_rank + self.rng.randint(-100, 100))
        opponent_id = f"u_arena_{self.rng.randint(1, 100000):06d}"

        attempt_num = 5 - agent.arena_attempts_today

        self.emitter.emit_arena_battle_start(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            opponent_user_id=opponent_id,
            opponent_power=opponent_power,
            opponent_rank=opponent_rank,
            player_power=agent.team_power,
            player_rank=agent.arena_rank,
            attempt_number=attempt_num,
            is_paid_attempt=is_paid,
        )
        agent.current_session_events += 1

        # Battle
        duration = self.rng.randint(30, 90)
        end_time = timestamp + timedelta(seconds=duration)

        won = self.behavior.simulate_arena_result(agent, opponent_power, self.rng)
        rating_change = self.behavior.calculate_arena_rating_change(
            agent.arena_rating, 1000, won
        )

        old_rank = agent.arena_rank
        agent.arena_rating += rating_change
        agent.arena_rank = max(1, int(2000 - agent.arena_rating / 10))

        reward_currency = None
        reward_amount = None
        if won:
            reward_currency = "gold"
            reward_amount = 100 + agent.arena_rank
            agent.gold += reward_amount

        self.emitter.emit_arena_battle_end(
            agent=agent,
            timestamp=end_time,
            current_date=self.current_date,
            opponent_user_id=opponent_id,
            result="win" if won else "lose",
            duration_sec=duration,
            rank_before=old_rank,
            rank_after=agent.arena_rank,
            rating_change=rating_change,
            reward_currency=reward_currency,
            reward_amount=reward_amount,
        )
        agent.current_session_events += 1

        if won:
            self.emitter.emit_economy_source(
                agent=agent,
                timestamp=end_time,
                current_date=self.current_date,
                currency="gold",
                amount=reward_amount,
                balance_after=agent.gold,
                source="arena_reward",
            )
            agent.current_session_events += 1

            # Update quest
            for quest in agent.daily_quests:
                if quest.quest_id == "dq_arena" and not quest.completed:
                    quest.current += 1
                    if quest.current >= quest.target:
                        quest.completed = True

        return end_time

    def _attack_guild_boss(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Attack guild boss."""
        if not agent.guild_id or agent.attacked_guild_boss_today:
            return timestamp

        guild = None
        for g in self.world.guilds:
            if g.guild_id == agent.guild_id:
                guild = g
                break

        if not guild:
            return timestamp

        # Calculate damage (based on team power)
        damage_pct = agent.team_power / 1000 * self.rng.uniform(0.8, 1.2)
        damage_pct = min(damage_pct, 10.0)  # Max 10% per hit
        damage_dealt = int(damage_pct * 10000)  # Scale for display

        hp_remaining = self.world.damage_guild_boss(agent.guild_id, damage_pct)

        self.emitter.emit_guild_boss_attack(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            guild_id=guild.guild_id,
            boss_id=f"boss_{guild.boss_level:03d}",
            boss_level=guild.boss_level,
            damage_dealt=damage_dealt,
            team_power=agent.team_power,
            attempt_number=1,
            boss_hp_remaining_pct=hp_remaining,
        )
        agent.current_session_events += 1

        agent.attacked_guild_boss_today = True

        # Reward
        reward_gold = 500 + guild.boss_level * 100
        agent.gold += reward_gold

        self.emitter.emit_economy_source(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            currency="gold",
            amount=reward_gold,
            balance_after=agent.gold,
            source="guild_reward",
        )
        agent.current_session_events += 1

        return timestamp + timedelta(seconds=self.rng.randint(30, 60))

    def _join_guild(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Join a guild."""
        if agent.guild_id:
            return timestamp

        guild = self.world.get_random_guild(self.rng)
        if not guild:
            return timestamp

        if self.world.join_guild(guild.guild_id):
            agent.guild_id = guild.guild_id
            agent.guild_joined_date = self.current_date

            self.emitter.emit_guild_join(
                agent=agent,
                timestamp=timestamp,
                current_date=self.current_date,
                guild=guild,
                join_method="search",
            )
            agent.current_session_events += 1

        return timestamp + timedelta(seconds=self.rng.randint(10, 30))

    def _watch_ad(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Watch rewarded ad."""
        if agent.ads_watched_today >= self.config.max_ads_per_day:
            return timestamp

        placement = self.rng.choice(["main_screen", "shop", "energy_refill"])
        ad_network = self.rng.choice(AD_NETWORKS)

        # Ad opportunity
        self.emitter.emit_ad_opportunity(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            placement=placement,
            ads_watched_today=agent.ads_watched_today,
            ads_available=self.config.max_ads_per_day - agent.ads_watched_today,
        )
        agent.current_session_events += 1

        timestamp += timedelta(seconds=2)

        # Ad started
        self.emitter.emit_ad_started(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            placement=placement,
            ad_network=ad_network,
        )
        agent.current_session_events += 1

        # Watch (skip small chance)
        if self.rng.random() < 0.05:  # 5% skip
            skip_time = self.rng.randint(5, 15)
            self.emitter.emit_ad_skipped(
                agent=agent,
                timestamp=timestamp + timedelta(seconds=skip_time),
                current_date=self.current_date,
                placement=placement,
                ad_network=ad_network,
                skip_after_sec=skip_time,
                skip_reason="user_closed",
            )
            agent.current_session_events += 1
            return timestamp + timedelta(seconds=skip_time)

        # Complete
        watch_duration = self.rng.randint(15, 30)
        end_time = timestamp + timedelta(seconds=watch_duration)

        # Get reward amount (may vary by A/B test)
        reward_amount = self.config.ad_reward_gems
        ad_test = agent.ab_tests.get("ad_reward_amount")
        if ad_test:
            effects = self.config.ab_tests.get("ad_reward_amount", {}).get("effects", {})
            reward_amount = effects.get(ad_test, {}).get("reward_gems", reward_amount)

        agent.gems += reward_amount
        agent.ads_watched_today += 1

        self.emitter.emit_ad_completed(
            agent=agent,
            timestamp=end_time,
            current_date=self.current_date,
            placement=placement,
            ad_network=ad_network,
            reward_currency="gems",
            reward_amount=reward_amount,
            watch_duration_sec=watch_duration,
        )
        agent.current_session_events += 1

        self.emitter.emit_economy_source(
            agent=agent,
            timestamp=end_time,
            current_date=self.current_date,
            currency="gems",
            amount=reward_amount,
            balance_after=agent.gems,
            source="ad_reward",
        )
        agent.current_session_events += 1

        return end_time

    def _browse_shop(self, agent: AgentState, timestamp: datetime) -> datetime:
        """Browse shop and potentially make purchase."""
        tab = self.rng.choice(["iap", "gems", "daily", "special"])

        self.emitter.emit_shop_view(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            shop_tab=tab,
            player_gems=agent.gems,
        )
        agent.current_session_events += 1

        timestamp += timedelta(seconds=self.rng.randint(5, 20))

        # Check for IAP trigger
        trigger = None
        if not agent.bought_starter_pack:
            trigger = "starter_pack_offer"
        elif agent.pity_counter >= 70:
            trigger = "pity_close"
        elif agent.energy < 20:
            trigger = "out_of_energy"
        elif not agent.has_active_monthly and self.rng.random() < 0.3:
            trigger = "monthly_pass_reminder"
        elif (self.current_date - agent.install_date).days >= 30:
            trigger = "late_game_offer"

        if trigger and self.behavior.should_attempt_iap(agent, trigger, self.rng):
            timestamp = self._make_purchase(agent, timestamp, trigger)

        return timestamp

    def _make_purchase(self, agent: AgentState, timestamp: datetime, trigger: str) -> datetime:
        """Make an IAP purchase."""
        product_id = self.behavior.select_iap_product(agent, trigger, self.rng)
        product_config = self.config.shop_products.get(product_id, {})

        price = product_config.get("price_usd", 0.99)
        product_name = PRODUCT_NAMES.get(product_id, product_id)

        # A/B test starter pack price
        if product_id == "starter_pack":
            test = agent.ab_tests.get("starter_pack_price")
            if test:
                effects = self.config.ab_tests.get("starter_pack_price", {}).get("effects", {})
                price = effects.get(test, {}).get("price_usd", price)

        # Initiated
        self.emitter.emit_iap_initiated(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            product_id=product_id,
            product_name=product_name,
            price_usd=price,
        )
        agent.current_session_events += 1

        timestamp += timedelta(seconds=self.rng.randint(5, 15))

        # Chance to fail/cancel
        if self.rng.random() < 0.1:  # 10% fail
            reason = self.rng.choice(["cancelled", "payment_error", "network_error"])
            self.emitter.emit_iap_failed(
                agent=agent,
                timestamp=timestamp,
                current_date=self.current_date,
                product_id=product_id,
                price_usd=price,
                fail_reason=reason,
            )
            agent.current_session_events += 1
            return timestamp

        # Success
        gems = product_config.get("gems", 0)
        if product_id == "monthly_pass":
            gems = product_config.get("gems_immediate", 300)

        items = []
        tickets = product_config.get("summon_tickets", 0)
        if tickets:
            items.append({"item_id": "summon_ticket", "amount": tickets})
            agent.summon_tickets += tickets

        agent.gems += gems
        agent.total_spent_usd += price
        agent.purchase_count += 1

        vip_points = int(price * 100)
        agent.vip_points += vip_points
        agent.vip_level = self.config.get_vip_level_for_spend(agent.total_spent_usd)

        is_first = agent.purchase_count == 1

        if product_id == "starter_pack":
            agent.bought_starter_pack = True
        elif product_id == "monthly_pass":
            agent.has_active_monthly = True
            agent.monthly_pass_start = self.current_date
            agent.monthly_pass_day = 0

        self.emitter.emit_iap_purchase(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            product_id=product_id,
            product_name=product_name,
            price_usd=price,
            gems_received=gems,
            items_received=items,
            is_first_purchase=is_first,
            purchase_number=agent.purchase_count,
            vip_points_earned=vip_points,
        )
        agent.current_session_events += 1

        self.emitter.emit_economy_source(
            agent=agent,
            timestamp=timestamp,
            current_date=self.current_date,
            currency="gems",
            amount=gems,
            balance_after=agent.gems,
            source="iap_purchase",
            source_id=product_id,
        )
        agent.current_session_events += 1

        return timestamp

    def _check_level_up(self, agent: AgentState, timestamp: datetime) -> None:
        """Check and process player level up."""
        while True:
            exp_needed = self.world.get_exp_for_level(agent.player_level + 1)
            if agent.player_exp < exp_needed:
                break
            if agent.player_level >= 100:
                break

            old_level = agent.player_level
            agent.player_exp -= exp_needed
            agent.player_level += 1

            # Check unlocks
            unlocks = []
            for feature, level in self.config.feature_unlocks.items():
                if level == agent.player_level:
                    unlocks.append(feature)

            self.emitter.emit_player_levelup(
                agent=agent,
                timestamp=timestamp,
                current_date=self.current_date,
                old_level=old_level,
                new_level=agent.player_level,
                unlocked_features=unlocks,
            )
            agent.current_session_events += 1

    def _flush_events(self) -> None:
        """Write accumulated events to output."""
        events = self.emitter.get_events()
        self.output.write_events(events)
        self.emitter.clear()
