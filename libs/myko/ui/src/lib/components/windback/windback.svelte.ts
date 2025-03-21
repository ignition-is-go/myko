import { client } from '$lib/services/client.js';
import { SetClientWindbackTime, WindbackStatus, type MItemStub } from '@myko/core';
import { DateTime } from 'luxon';

export class WindbackState {
	#ctx: MItemStub | undefined = $state();

	#liveWindbackTime: WindbackStatus['$reportResult'] = $state();

	constructor() {
		client.watchReport(new WindbackStatus()).subscribe((result) => {
			this.#liveWindbackTime = result;
		});
	}

	get ctx() {
		return this.#ctx;
	}

	set ctx(value: MItemStub | undefined) {
		this.#ctx = value;
	}

	get windingBack() {
		return !!this.#liveWindbackTime && !!this.#ctx;
	}

	get cursor(): DateTime {
		return this.#liveWindbackTime ? DateTime.fromISO(this.#liveWindbackTime) : DateTime.utc();
	}

	updateCursor(time: DateTime) {
		const valid = time.toUTC().toISO();
		if (!valid) {
			console.error('Invalid time', time);
			return;
		}
		client.sendCommand(new SetClientWindbackTime(valid));
	}
}

export const windbackState = new WindbackState();

export const startWindback = (root: MItemStub) => {
	console.log('Starting Windback', root);
	windbackState.ctx = root;

	client.sendCommand(new SetClientWindbackTime(DateTime.utc().minus({ hours: 3 }).toISO()));
};

export const exitWindback = () => {
	windbackState.ctx = undefined;
	// client.sendCommand(new ClearClientWindbackTime());
};
