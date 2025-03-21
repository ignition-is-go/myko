import { DateTime, Duration } from 'luxon';
import { memoizeWith, uniq } from 'ramda';
import { SvelteSet } from 'svelte/reactivity';

export class TransactionsViewState {
	#visibleEvents = new SvelteSet<string>();

	#timeZero: DateTime = $state(DateTime.now());

	#leftTimeMilis = $state(DateTime.now().minus({ minutes: 15 }).toMillis());

	#rightTimeMilis: number | null = $state(DateTime.now().toMillis());

	#widthPx: number = $state(0);

	#now: DateTime = $state(DateTime.now());

	#mouseX: number = $state(0);

	readonly #leftTime: DateTime = $derived(fromMillisMemo(this.#leftTimeMilis));
	readonly #rightTime: DateTime | null = $derived(
		this.#rightTimeMilis ? fromMillisMemo(this.#rightTimeMilis) : null
	);

	readonly #timeRangeMilis: number = $derived(this.rightTimeMilis - this.#leftTimeMilis);

	readonly #durationMilisPerPx = $derived(this.#timeRangeMilis / this.#widthPx);

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

	set rightTime(_value: DateTime) {
		throw new Error('Cannot set rightTime use rightTimeMilis');
	}

	get rightTimeMilis(): number {
		return this.#rightTimeMilis ?? this.#now.toMillis();
	}

	set rightTimeMilis(_value: number) {
		throw new Error('Cannot set rightTimeMilis. [readonly]');
	}

	get durationMilisPerPx(): number {
		return this.#durationMilisPerPx;
	}

	set durationMilisPerPx(_value: number) {
		throw new Error('Cannot set durationMilisPerPx. [readonly]');
	}

	get leftTime(): DateTime {
		return this.#leftTime;
	}

	set leftTime(_value: DateTime) {
		throw new Error('Cannot set leftTime [readonly]');
	}

	get leftTimeMilis(): number {
		return this.#leftTimeMilis;
	}

	set leftTimeMilis(_value: number) {
		throw new Error('Cannot set leftTimeMilis [readonly]');
	}

	get timeRangeMilis(): number {
		return this.#timeRangeMilis;
	}

	set timeRangeMilis(_value: number) {
		throw new Error('Cannot set timeRangeMilis [readonly]');
	}

	get milisPerPx(): number {
		return this.#timeRangeMilis / this.#widthPx;
	}

	set milisPerPx(_value: number) {
		throw new Error('Cannot set milisPerPx [readonly]');
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

	set now(_value: DateTime) {
		throw new Error('Cannot set now [readonly]');
	}

	set mouseX(value: number) {
		this.#mouseX = value;
	}

	get mouseX(): number {
		throw new Error('why are you getting mouseX?');
	}

	get allEventTimestamps(): number[] {
		const times = Array.from(this.#visibleEvents.values()).map((ts) => fromISOMemo(ts).toMillis());

		const adjustedMs = times.map((t) => t - this.#timeZero.toMillis());

		const remap = (t: number) => {
			return (t / (this.#now.toMillis() - this.#timeZero.toMillis())) * this.#widthPx;
		};

		const adjustedPx = adjustedMs.map(remap).map(Math.round);

		return uniq(adjustedPx);
	}

	set allEventTimestamps(_value: number[]) {
		throw new Error('Cannot set allEventTimestamps [readonly]');
	}

	get majorResolution(): Resolution {
		return resolutions[this.majorResolutionIndex];
	}

	get majorResolutionIndex(): number {
		const real = resolutions.findIndex(
			(resolution) => resolution.milis * (1 / this.#durationMilisPerPx) >= 100
		);

		const safe = Math.max(0, Math.min(real, resolutions.length - 1));

		return safe;
	}

	get majors() {
		const majorWidth = this.majorResolution.milis / this.durationMilisPerPx;

		const majorLefts = this.width / majorWidth + 2;

		const firstMajorTime = this.leftTimeMilis - (this.leftTimeMilis % this.majorResolution.milis);

		const majorTimes = Array.from({ length: majorLefts }).map(
			(_, i) => firstMajorTime + i * this.majorResolution.milis
		);

		const majorBundles = majorTimes.map((x) => {
			const leftTimeMilis = x - this.leftTimeMilis;
			const leftPx = leftTimeMilis / this.durationMilisPerPx;
			return {
				leftPx,
				time: x
			};
		});

		return majorBundles;
	}

	get minorResolution(): Resolution {
		const minorIndex = this.majorResolutionIndex - 1;
		if (minorIndex < 0) {
			return resolutions[0];
		}

		if (minorIndex > resolutions.length - 1) {
			return resolutions[resolutions.length - 1];
		}

		return resolutions[minorIndex];
	}

	get minors() {
		const numMinors = this.majorResolution.milis / this.minorResolution.milis - 1;

		const minorWidth = this.minorResolution.milis / this.durationMilisPerPx;

		const minorLefts = Array.from({ length: numMinors }).map((_, i) => ({
			leftPx: (i + 1) * minorWidth,
			time: (i + 1) * this.minorResolution.milis
		}));

		return minorLefts;
	}

	zoomAllTheWayOut() {
		this.#rightTimeMilis = this.#now.toMillis();
		this.#leftTimeMilis = this.#timeZero.toMillis();
	}

	zoom(delta: number) {
		const factor = 1 + delta / 1000;
		const oldTimeRangeMilis = this.#timeRangeMilis;
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

		this.#rightTimeMilis = newRightTimeMilis;

		this.#leftTimeMilis = newLeftTimeMilis;
	}

	pan(delta: number) {
		const factor = delta / -5000;

		const timeRangeMilis = this.#timeRangeMilis;

		const timeDiff = timeRangeMilis * factor;

		const newLeftTimeMilis = this.leftTime.toMillis() + timeDiff;
		const newRightTimeMilis = this.rightTime.toMillis() + timeDiff;

		const nowMilis = DateTime.now().toMillis();
		const zeroMilis = this.#timeZero.toMillis();

		if (newRightTimeMilis > nowMilis) {
			const diff = newRightTimeMilis - nowMilis;

			const adjustedNewLeftTimeMilis = newLeftTimeMilis - diff;

			this.#rightTimeMilis = nowMilis;
			this.#leftTimeMilis = adjustedNewLeftTimeMilis;

			return;
		}

		if (newLeftTimeMilis < zeroMilis) {
			const diff = zeroMilis - newLeftTimeMilis;

			const adjustedNewRightTimeMilis = newRightTimeMilis + diff;

			this.#leftTimeMilis = zeroMilis;
			this.#rightTimeMilis = adjustedNewRightTimeMilis;

			return;
		}

		this.#leftTimeMilis = newLeftTimeMilis;
		this.#rightTimeMilis = newRightTimeMilis;
	}
}

export const TRANSACTIONS_VIEW_STATE = Symbol('transactions-view-state');

export const FULL_DATE_FORMAT = `yyyy-MM-dd HH:mm:ss`;

export const fromISOMemo = memoizeWith((isoString) => isoString, DateTime.fromISO);
export const fromMillisMemo = memoizeWith((millis) => millis.toString(), DateTime.fromMillis);
export const isoToMillisMemo = memoizeWith(
	(isoString) => isoString,
	(isoString: string) => DateTime.fromISO(isoString).toMillis()
);

export type Resolution = {
	milis: number;
	majorFormat: string;
	minorFormat: string;
};

const resolutions = [
	{
		label: '1ms',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		minorFormat: '.SSS',
		milis: Duration.fromObject({ milliseconds: 1 }).as('milliseconds')
	},
	{
		label: '5ms',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		minorFormat: '.SSS',
		milis: Duration.fromObject({ milliseconds: 5 }).as('milliseconds')
	},
	{
		label: '10ms',
		minorFormat: '.SSS',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		milis: Duration.fromObject({ milliseconds: 10 }).as('milliseconds')
	},
	{
		label: '50ms',
		minorFormat: '.SSS',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		milis: Duration.fromObject({ milliseconds: 50 }).as('milliseconds')
	},
	{
		label: '100ms',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		minorFormat: '.SSS',

		milis: Duration.fromObject({ milliseconds: 100 }).as('milliseconds')
	},
	{
		label: '500ms',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		minorFormat: '.SSS',
		milis: Duration.fromObject({ milliseconds: 500 }).as('milliseconds')
	},
	{
		label: '1s',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'ss',
		milis: Duration.fromObject({ seconds: 1 }).as('milliseconds')
	},
	{
		label: '5s',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'ss',
		milis: Duration.fromObject({ seconds: 5 }).as('milliseconds')
	},
	{
		label: '10s',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'ss',
		milis: Duration.fromObject({ seconds: 10 }).as('milliseconds')
	},
	{
		label: '30s',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'ss',
		milis: Duration.fromObject({ seconds: 30 }).as('milliseconds')
	},
	{
		label: '1m',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'mm:ss',
		milis: Duration.fromObject({ minutes: 1 }).as('milliseconds')
	},
	{
		label: '5m',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'mm:ss',
		milis: Duration.fromObject({ minutes: 5 }).as('milliseconds')
	},
	{
		label: '10m',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'mm:ss',
		milis: Duration.fromObject({ minutes: 10 }).as('milliseconds')
	},
	{
		label: '30m',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'mm:ss',
		milis: Duration.fromObject({ minutes: 30 }).as('milliseconds')
	},
	{
		label: '1h',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'HH:mm',
		milis: Duration.fromObject({ hours: 1 }).as('milliseconds')
	},
	{
		label: '6h',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'HH:mm',
		milis: Duration.fromObject({ hours: 6 }).as('milliseconds')
	},
	{
		label: '12h',
		majorFormat: 'dd MMM',
		minorFormat: 'HH:mm',
		milis: Duration.fromObject({ hours: 12 }).as('milliseconds')
	},
	{
		label: '1d',
		majorFormat: 'dd MMM',
		minorFormat: 'dd',
		milis: Duration.fromObject({ days: 1 }).as('milliseconds')
	},
	{
		label: '1w',
		majorFormat: 'dd MMM yyyy',
		minorFormat: 'dd',
		milis: Duration.fromObject({ weeks: 1 }).as('milliseconds')
	},
	{
		label: '1M',
		majorFormat: 'MMM yyyy',
		minorFormat: 'MMM',
		milis: Duration.fromObject({ months: 1 }).as('milliseconds')
	},
	{
		label: '1y',
		majorFormat: 'yyyy',
		minorFormat: 'yyyy',
		milis: Duration.fromObject({ years: 1 }).as('milliseconds')
	}
];
