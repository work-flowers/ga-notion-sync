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

async function getServiceAccountToken(scope: string): Promise<string> {
	const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
	if (!raw) {
		throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON secret is not set");
	}

	const key = JSON.parse(raw) as ServiceAccountKey;
	const tokenUri = key.token_uri ?? "https://oauth2.googleapis.com/token";

	const now = Math.floor(Date.now() / 1000);
	const header = { alg: "RS256", typ: "JWT" };
	const claims = {
		iss: key.client_email,
		scope,
		aud: tokenUri,
		iat: now,
		exp: now + 3600,
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

	const data = (await response.json()) as { access_token: string };
	return data.access_token;
}

const GA4_SCOPE = "https://www.googleapis.com/auth/analytics.readonly";

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
	d.setDate(d.getDate() - n);
	return d.toISOString().slice(0, 10);
}

// --- Shared constants ---

const BATCH_SIZE = 250;
const LOOKBACK_DAYS = 30;

interface SyncState {
	offset: number;
}

// --- Pages Path Report Sync ---

const pagesReportConfig: GA4ReportConfig = {
	dimensions: ["date", "pagePath"],
	metrics: ["screenPageViews", "newUsers", "totalUsers", "userEngagementDuration"],
};

worker.sync("pagesPathReport", {
	database: pagesDb,
	schedule: "1h",
	mode: "replace",
	execute: async (state: SyncState | undefined) => {
		const propertyId = process.env.GA4_PROPERTY_ID;
		if (!propertyId) {
			throw new Error("GA4_PROPERTY_ID secret is not set");
		}

		const token = await getServiceAccountToken(GA4_SCOPE);
		const offset = state?.offset ?? 0;
		const startDate = getDateNDaysAgo(LOOKBACK_DAYS);
		const endDate = getDateNDaysAgo(1);

		const { rows, totalRows } = await fetchGA4Report(
			token, propertyId, pagesReportConfig, startDate, endDate, BATCH_SIZE, offset,
		);

		const changes = rows.map((row) => {
			const date = row.dimensionValues[0].value;
			const pagePath = row.dimensionValues[1].value;
			return {
				type: "upsert" as const,
				key: `${date}::${pagePath}`,
				properties: {
					"Name": Builder.title(`${date}::${pagePath}`),
					"Date": Builder.date(formatGA4Date(date)),
					"Page Path": Builder.richText(pagePath),
					"Screen Page Views": Builder.number(parseInt(row.metricValues[0].value, 10)),
					"New Users": Builder.number(parseInt(row.metricValues[1].value, 10)),
					"Total Users": Builder.number(parseInt(row.metricValues[2].value, 10)),
					"User Engagement Duration": Builder.number(parseFloat(row.metricValues[3].value)),
				},
			};
		});

		const nextOffset = offset + rows.length;
		const hasMore = nextOffset < totalRows;

		return {
			changes,
			hasMore,
			nextState: hasMore ? { offset: nextOffset } : undefined,
		};
	},
});

// --- Traffic Session Source Medium Report Sync ---

const trafficReportConfig: GA4ReportConfig = {
	dimensions: ["date", "sessionSource", "sessionMedium"],
	metrics: ["sessions", "totalUsers"],
};

worker.sync("trafficSourceMediumReport", {
	database: trafficDb,
	schedule: "1h",
	mode: "replace",
	execute: async (state: SyncState | undefined) => {
		const propertyId = process.env.GA4_PROPERTY_ID;
		if (!propertyId) {
			throw new Error("GA4_PROPERTY_ID secret is not set");
		}

		const token = await getServiceAccountToken(GA4_SCOPE);
		const offset = state?.offset ?? 0;
		const startDate = getDateNDaysAgo(LOOKBACK_DAYS);
		const endDate = getDateNDaysAgo(1);

		const { rows, totalRows } = await fetchGA4Report(
			token, propertyId, trafficReportConfig, startDate, endDate, BATCH_SIZE, offset,
		);

		const changes = rows.map((row) => {
			const date = row.dimensionValues[0].value;
			const source = row.dimensionValues[1].value;
			const medium = row.dimensionValues[2].value;
			return {
				type: "upsert" as const,
				key: `${date}::${source}::${medium}`,
				properties: {
					"Name": Builder.title(`${date}::${source}::${medium}`),
					"Date": Builder.date(formatGA4Date(date)),
					"Session Source": Builder.richText(source),
					"Session Medium": Builder.richText(medium),
					"Sessions": Builder.number(parseInt(row.metricValues[0].value, 10)),
					"Users": Builder.number(parseInt(row.metricValues[1].value, 10)),
				},
			};
		});

		const nextOffset = offset + rows.length;
		const hasMore = nextOffset < totalRows;

		return {
			changes,
			hasMore,
			nextState: hasMore ? { offset: nextOffset } : undefined,
		};
	},
});
