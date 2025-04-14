import { client } from '$lib/services/client.js';
import { SetClientWindbackTime, WindbackStatus, type MItemStub } from '@myko/core';
import { DateTime } from 'luxon';

export class WindbackState {
	#ctx: Omit<MItemStub, 'hash'> | undefined = $state();

	#liveWindbackTime: WindbackStatus['$reportResult'] = $state();

	#localWindbackTime: DateTime | undefined = $state();

	constructor() {
		client.watchReport(new WindbackStatus()).subscribe((result) => {
			this.#liveWindbackTime = result;
			this.#localWindbackTime = result ? DateTime.fromISO(result) : undefined;
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
		client.sendCommand(new SetClientWindbackTime(valid));
	}
}

export const windbackState = new WindbackState();

export const startWindback = (root: Omit<MItemStub, 'hash'>) => {
	client
		.sendCommand(new SetClientWindbackTime(DateTime.utc().minus({ seconds: 5 }).toISO()))

		.then((x) => {
			if (!x) {
				return;
			}
			windbackState.ctx = root;
		});
};

export const exitWindback = () => {
	windbackState.ctx = undefined;
	// client.sendCommand(new ClearClientWindbackTime());
};
