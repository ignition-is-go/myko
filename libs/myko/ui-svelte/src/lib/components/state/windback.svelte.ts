import { myko as client } from '../../services/svelte-client.svelte.js';
import {
	ClearClientWindbackTime,
	SetClientWindbackTime,
	WindbackStatus,
	type MItemStub,
	type WindbackStatusOutput
} from '@myko/core';
import { DateTime } from 'luxon';

export class WindbackState {
	#ctx: Omit<MItemStub, 'hash'> | undefined = $state();

	#liveWindbackTime: string | null = $state(null);

	#localWindbackTime: DateTime | undefined = $state();

	constructor() {
		client.watchReport(new WindbackStatus({})).subscribe((result: WindbackStatusOutput) => {
			this.#liveWindbackTime = result.windback ?? null;
			this.#localWindbackTime = result.windback ? DateTime.fromISO(result.windback) : undefined;
		});
	}

	get ctx(): Omit<MItemStub, 'hash'> | undefined {
		return this.#ctx;
	}

	set ctx(value: Omit<MItemStub, 'hash'> | undefined) {
		this.#ctx = value;
	}

	get windingBack() {
		return !!this.#liveWindbackTime && !!this.#ctx;
	}

	get cursor(): DateTime | undefined {
		return this.#localWindbackTime ? this.#localWindbackTime : undefined;
	}

	get caughtUp() {
		console.log('Caught up', this.#liveWindbackTime, this.#localWindbackTime?.toUTC()?.toISO());
		return this.#liveWindbackTime === this.#localWindbackTime?.toUTC()?.toISO();
	}

	updateCursor(time: DateTime) {
		this.#localWindbackTime = time;
	}

	saveWindbackTime() {
		if (!this.#ctx) {
			return;
		}

		if (!this.#localWindbackTime) {
			return;
		}

		const valid = this.#localWindbackTime.toUTC().toISO();

		if (!valid) {
			console.error('Invalid time', this.#localWindbackTime);
			return;
		}
		client.sendCommand(new SetClientWindbackTime({ windback: valid }));
	}
}

export const windbackState = new WindbackState();

export const startWindback = (root: Omit<MItemStub, 'hash'>) => {
	const windback = DateTime.utc().minus({ seconds: 5 }).toISO();
	if (!windback) return;

	client
		.sendCommand(new SetClientWindbackTime({ windback }))
		.then((x) => {
			if (!x) {
				return;
			}
			windbackState.ctx = root;
		});
};

export const exitWindback = () => {
	windbackState.ctx = undefined;
	client.sendCommand(new ClearClientWindbackTime({}));
};
