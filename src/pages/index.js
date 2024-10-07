import Head from 'next/head';
import Counter from '../components/Counter';

export default function Home() {
  return (
    <div>
      <Head>
        <title>Counter Example</title>
        <meta name="description" content="A simple counter example using Next.js" />
        <link rel="icon" href="/favicon.ico" />
      </Head>

      <main>
        <h1>Counter Example</h1>
        <Counter />
      </main>
    </div>
  );
}
