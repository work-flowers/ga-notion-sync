import { Worker } from "@notionhq/workers";
import * as Schema from "@notionhq/workers/schema";
import * as Builder from "@notionhq/workers/builder";
import { createSign } from "node:crypto";

const worker = new Worker();
export default worker;

// --- Service account auth ---

interface ServiceAccountKey {
	client_email: string;
	private_key: string;
	token_uri?: string;
}

function base64url(input: Buffer | string): string {
	return Buffer.from(input)
		.toString("base64")
		.replace(/=+$/, "")
		.replace(/\+/g, "-")
		.replace(/\//g, "_");
}

let cachedAccessToken: { token: string; expiresAt: number } | null = null;

async function getServiceAccountToken(scope: string): Promise<string> {
	const nowMs = Date.now();
	if (cachedAccessToken && cachedAccessToken.expiresAt > nowMs + 60_000) {
		return cachedAccessToken.token;
	}

	const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
	if (!raw) {
		throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON secret is not set");
	}

	const key = JSON.parse(raw) as ServiceAccountKey;
	const tokenUri = key.token_uri ?? "https://oauth2.googleapis.com/token";

	const iat = Math.floor(nowMs / 1000);
	const header = { alg: "RS256", typ: "JWT" };
	const claims = {
		iss: key.client_email,
		scope,
		aud: tokenUri,
		iat,
		exp: iat + 3600,
	};

	const signingInput = `${base64url(JSON.stringify(header))}.${base64url(JSON.stringify(claims))}`;
	const signer = createSign("RSA-SHA256");
	signer.update(signingInput);
	signer.end();
	const signature = base64url(signer.sign(key.private_key));
	const jwt = `${signingInput}.${signature}`;

	const response = await fetch(tokenUri, {
		method: "POST",
		headers: { "Content-Type": "application/x-www-form-urlencoded" },
		body: new URLSearchParams({
			grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
			assertion: jwt,
		}),
	});

	if (!response.ok) {
		const body = await response.text();
		throw new Error(`Token exchange failed ${response.status}: ${body}`);
	}

	const data = (await response.json()) as { access_token: string; expires_in?: number };
	const expiresInMs = (data.expires_in ?? 3600) * 1000;
	cachedAccessToken = { token: data.access_token, expiresAt: nowMs + expiresInMs };
	return data.access_token;
}

const GA4_SCOPE = "https://www.googleapis.com/auth/analytics.readonly";

// --- Pacer ---

const ga4Pacer = worker.pacer("ga4Api", {
	allowedRequests: 100,
	intervalMs: 60_000,
});

// --- Databases ---

const pagesDb = worker.database("pagesPathReportDb", {
	type: "managed",
	initialTitle: "Pages Path Report",
	primaryKeyProperty: "Name",
	schema: {
		databaseIcon: Builder.emojiIcon("📄"),
		properties: {
			"Name": Schema.title(),
			"Date": Schema.date("YYYY/MM/DD"),
			"Page Path": Schema.richText(),
			"Screen Page Views": Schema.number(),
			"New Users": Schema.number(),
			"Total Users": Schema.number(),
			"User Engagement Duration": Schema.number(),
		},
	},
});

const trafficDb = worker.database("trafficSourceMediumDb", {
	type: "managed",
	initialTitle: "Traffic Session Source Medium Report",
	primaryKeyProperty: "Name",
	schema: {
		databaseIcon: Builder.emojiIcon("🚥"),
		properties: {
			"Name": Schema.title(),
			"Date": Schema.date("YYYY/MM/DD"),
			"Session Source": Schema.richText(),
			"Session Medium": Schema.richText(),
			"Sessions": Schema.number(),
			"Users": Schema.number(),
		},
	},
});

// --- GA4 API helper ---

interface GA4ReportConfig {
	dimensions: string[];
	metrics: string[];
}

interface GA4RawRow {
	dimensionValues: Array<{ value: string }>;
	metricValues: Array<{ value: string }>;
}

interface GA4RawResponse {
	rows: GA4RawRow[];
	totalRows: number;
}

async function fetchGA4Report(
	token: string,
	propertyId: string,
	config: GA4ReportConfig,
	startDate: string,
	endDate: string,
	limit: number,
	offset: number,
): Promise<GA4RawResponse> {
	const url = `https://analyticsdata.googleapis.com/v1beta/properties/${propertyId}:runReport`;

	const response = await fetch(url, {
		method: "POST",
		headers: {
			Authorization: `Bearer ${token}`,
			"Content-Type": "application/json",
		},
		body: JSON.stringify({
			dateRanges: [{ startDate, endDate }],
			dimensions: config.dimensions.map((name) => ({ name })),
			metrics: config.metrics.map((name) => ({ name })),
			limit,
			offset,
		}),
	});

	if (!response.ok) {
		const body = await response.text();
		throw new Error(`GA4 API error ${response.status}: ${body}`);
	}

	const data = (await response.json()) as {
		rows?: GA4RawRow[];
		rowCount?: number;
	};

	return {
		rows: data.rows ?? [],
		totalRows: data.rowCount ?? 0,
	};
}

function formatGA4Date(yyyymmdd: string): string {
	return `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;
}

function getDateNDaysAgo(n: number): string {
	const d = new Date();
	d.setUTCDate(d.getUTCDate() - n);
	return d.toISOString().slice(0, 10);
}

// --- Shared sync builder ---

const BATCH_SIZE = 250;
// Backfill window starts before GA4's earliest practical data. Out-of-range
// dates just return empty rows, so this is safe.
const BACKFILL_START_DATE = "2020-01-01";
// Delta refetches a few days back from yesterday — GA4 metrics for the most
// recent days are still settling (late hits, attribution).
const DELTA_LOOKBACK_DAYS = 3;

interface SyncState {
	offset: number;
}

function makeGA4Execute<C>(opts: {
	config: GA4ReportConfig;
	mapRow: (row: GA4RawRow) => C;
	startDate: () => string;
	endDate: () => string;
}) {
	return async (state: SyncState | undefined) => {
		const propertyId = process.env.GA4_PROPERTY_ID;
		if (!propertyId) {
			throw new Error("GA4_PROPERTY_ID secret is not set");
		}

		const offset = state?.offset ?? 0;

		await ga4Pacer.wait();
		const token = await getServiceAccountToken(GA4_SCOPE);
		const { rows, totalRows } = await fetchGA4Report(
			token,
			propertyId,
			opts.config,
			opts.startDate(),
			opts.endDate(),
			BATCH_SIZE,
			offset,
		);

		const changes = rows.map(opts.mapRow);

		const nextOffset = offset + rows.length;
		// Guard against a no-progress loop if the API returns 0 rows but a
		// non-zero totalRows.
		const hasMore = rows.length > 0 && nextOffset < totalRows;

		return {
			changes,
			hasMore,
			nextState: hasMore ? { offset: nextOffset } : undefined,
		};
	};
}

// --- Pages Path Report ---

const pagesReportConfig: GA4ReportConfig = {
	dimensions: ["date", "pagePath"],
	metrics: ["screenPageViews", "newUsers", "totalUsers", "userEngagementDuration"],
};

function mapPagesRow(row: GA4RawRow) {
	const date = row.dimensionValues[0].value;
	const pagePath = row.dimensionValues[1].value;
	const key = `${date}::${pagePath}`;
	return {
		type: "upsert" as const,
		key,
		properties: {
			"Name": Builder.title(key),
			"Date": Builder.date(formatGA4Date(date)),
			"Page Path": Builder.richText(pagePath),
			"Screen Page Views": Builder.number(parseInt(row.metricValues[0].value, 10)),
			"New Users": Builder.number(parseInt(row.metricValues[1].value, 10)),
			"Total Users": Builder.number(parseInt(row.metricValues[2].value, 10)),
			"User Engagement Duration": Builder.number(parseFloat(row.metricValues[3].value)),
		},
	};
}

// Backfill: full history, triggered manually. Used for initial load,
// schema migrations, and recovery.
worker.sync("pagesPathBackfill", {
	database: pagesDb,
	mode: "replace",
	schedule: "manual",
	execute: makeGA4Execute({
		config: pagesReportConfig,
		mapRow: mapPagesRow,
		startDate: () => BACKFILL_START_DATE,
		endDate: () => getDateNDaysAgo(1),
	}),
});

// Delta: refresh the most recent days every hour. Incremental — never deletes.
worker.sync("pagesPathDelta", {
	database: pagesDb,
	mode: "incremental",
	schedule: "1h",
	execute: makeGA4Execute({
		config: pagesReportConfig,
		mapRow: mapPagesRow,
		startDate: () => getDateNDaysAgo(DELTA_LOOKBACK_DAYS),
		endDate: () => getDateNDaysAgo(1),
	}),
});

// --- Traffic Source/Medium Report ---

const trafficReportConfig: GA4ReportConfig = {
	dimensions: ["date", "sessionSource", "sessionMedium"],
	metrics: ["sessions", "totalUsers"],
};

function mapTrafficRow(row: GA4RawRow) {
	const date = row.dimensionValues[0].value;
	const source = row.dimensionValues[1].value;
	const medium = row.dimensionValues[2].value;
	const key = `${date}::${source}::${medium}`;
	return {
		type: "upsert" as const,
		key,
		properties: {
			"Name": Builder.title(key),
			"Date": Builder.date(formatGA4Date(date)),
			"Session Source": Builder.richText(source),
			"Session Medium": Builder.richText(medium),
			"Sessions": Builder.number(parseInt(row.metricValues[0].value, 10)),
			"Users": Builder.number(parseInt(row.metricValues[1].value, 10)),
		},
	};
}

worker.sync("trafficSourceMediumBackfill", {
	database: trafficDb,
	mode: "replace",
	schedule: "manual",
	execute: makeGA4Execute({
		config: trafficReportConfig,
		mapRow: mapTrafficRow,
		startDate: () => BACKFILL_START_DATE,
		endDate: () => getDateNDaysAgo(1),
	}),
});

worker.sync("trafficSourceMediumDelta", {
	database: trafficDb,
	mode: "incremental",
	schedule: "1h",
	execute: makeGA4Execute({
		config: trafficReportConfig,
		mapRow: mapTrafficRow,
		startDate: () => getDateNDaysAgo(DELTA_LOOKBACK_DAYS),
		endDate: () => getDateNDaysAgo(1),
	}),
});
