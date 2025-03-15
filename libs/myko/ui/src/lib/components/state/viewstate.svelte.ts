import { DateTime, Duration } from 'luxon';
import { uniq } from 'ramda';
import { SvelteSet } from 'svelte/reactivity';

export class TransactionsViewState {
	#visibleEvents = new SvelteSet<string>();

	#timeZero: DateTime = $state(DateTime.now());

	// #timeRange: Duration = $state(Duration.fromObject({ minutes: 15 }));

	#leftTime: DateTime = $state(DateTime.now().minus({ minutes: 15 }));
	#rightTime: DateTime | null = $state(DateTime.now());
	readonly #timeRange: Duration = $derived(this.rightTime.diff(this.#leftTime));

	#widthPx: number = $state(0);

	#durationPerPx: Duration = $derived(
		Duration.fromMillis(this.#timeRange.as('milliseconds') / this.#widthPx)
	);

	#now: DateTime = $state(DateTime.now());

	#mouseX: number = $state(0);

	constructor() {
		const updateTime = () => {
			this.#now = DateTime.now();

			requestAnimationFrame(updateTime);
		};

		updateTime();
	}

	registerEvents(events: string[]) {
		events.forEach((event) => this.#visibleEvents.add(event));
	}

	get rightTime(): DateTime {
		return this.#rightTime ?? this.#now;
	}

	set rightTime(value: DateTime) {
		this.#rightTime = value;
	}

	get leftTime(): DateTime {
		return this.rightTime.minus(this.#timeRange);
	}

	get durationPerPx(): Duration {
		return this.#durationPerPx;
	}

	set width(value: number) {
		this.#widthPx = value;
	}

	get width(): number {
		return this.#widthPx;
	}

	set timeZero(value: DateTime) {
		this.#timeZero = value;
	}

	get timeZero(): DateTime {
		return this.#timeZero;
	}

	get now(): DateTime {
		return this.#now;
	}

	set mouseX(value: number) {
		this.#mouseX = value;
	}

	get allEventTimestamps(): number[] {
		const times = Array.from(this.#visibleEvents.values()).map((ts) =>
			DateTime.fromISO(ts).toMillis()
		);

		const adjustedMs = times.map((t) => t - this.#timeZero.toMillis());

		const remap = (t: number) => {
			return (t / (this.#now.toMillis() - this.#timeZero.toMillis())) * this.#widthPx;
		};

		const adjustedPx = adjustedMs.map(remap).map(Math.round);

		return uniq(adjustedPx);
	}

	zoomAllTheWayOut() {
		this.#rightTime = this.#now;
		this.#leftTime = this.#timeZero;
	}

	zoom(delta: number) {
		const factor = 1 + delta / 1000;
		const oldTimeRangeMilis = this.#timeRange.as('milliseconds');
		const newTimeRangeMilis = oldTimeRangeMilis * factor;

		const normX = this.#mouseX / this.#widthPx;

		const timeRangeDiff = newTimeRangeMilis - oldTimeRangeMilis;

		const rightTimeDiff = timeRangeDiff * (1 - normX);
		const leftTimeDiff = timeRangeDiff * normX;

		const nowMilis = DateTime.now().toMillis();
		const leftTimeMilis = this.leftTime.toMillis();
		const rightTimeMilis = this.rightTime.toMillis();

		const newRightTimeMilis = Math.min(rightTimeMilis + rightTimeDiff, nowMilis);
		const newLeftTimeMilis = Math.max(leftTimeMilis - leftTimeDiff, this.#timeZero.toMillis());

		this.#rightTime = DateTime.fromMillis(newRightTimeMilis);
		this.#leftTime = DateTime.fromMillis(newLeftTimeMilis);
	}

	pan(delta: number) {
		const factor = delta / -5000;

		const timeRangeMilis = this.#timeRange.as('milliseconds');

		const timeDiff = timeRangeMilis * factor;

		const newLeftTimeMilis = this.leftTime.toMillis() + timeDiff;
		const newRightTimeMilis = this.rightTime.toMillis() + timeDiff;

		const nowMilis = DateTime.now().toMillis();
		const zeroMilis = this.#timeZero.toMillis();

		if (newRightTimeMilis > nowMilis) {
			return;
		}

		if (newLeftTimeMilis < zeroMilis) {
			return;
		}

		this.#leftTime = DateTime.fromMillis(newLeftTimeMilis);
		this.#rightTime = DateTime.fromMillis(newRightTimeMilis);
	}
}

export const TRANSACTIONS_VIEW_STATE = Symbol('transactions-view-state');

export const FULL_DATE_FORMAT = `yyyy-MM-dd HH:mm:ss`;
